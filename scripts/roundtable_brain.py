#!/usr/bin/env python3
"""円卓の正本から、全AI共通の検索可能な共有脳を生成する。

原本は変更しない。円卓JSONL、解決台帳、検証済み記録、記録索引を読み、
追記専用episodesと再生成可能なsnapshot/report/RAG copyを作る。
"""

from __future__ import annotations

import argparse
import contextlib
import contextvars
import copy
import fcntl
import hashlib
import time as _jikan_ai3170
import json
import math
import os
import re
import secrets
import subprocess
import sys
import tempfile
import threading
import unicodedata
from datetime import datetime, timedelta, timezone
from pathlib import Path

import roundtable_media
try:
    import ou_no_kokoromi
except Exception:
    ou_no_kokoromi = None
try:
    import nirai_adaptive
except Exception:
    nirai_adaptive = None
from nirai_record_roles import enrich_results as enrich_record_roles
try:
    import brain_snapshot_store as _snap_store
except Exception:
    _snap_store = None
from brain_bm25 import (
    BM25_K1, BM25_B, BM25_MAX_CACHED_TERMS as _BM25_MAX_CACHED_TERMS,
    build_hay_norms as _build_hay_norms, bm25_taat_scores as _bm25_taat_scores,
    build_incremental_bm25_carry as _build_incremental_bm25_carry,
)
from brain_text_utils import (
    KANA_ONLY, KATAMARI, GYOU_KIRI_CHUKI,
    normalize, query_terms, item_key,
    _nagasa, midashi_de_kiru, gyou_de_kiru,
)
from brain_cpu_agent import (
    BRAIN_MODE_ENV, CPU_AGENT_DEFAULT_LIMIT, AGENT_CONFIDENCE_SCHEMA,
    LOW_RR_SPEC_ABSTAIN_KITEI, LOW_RR_SPEC_COV_KITEI,
    resolve_brain_mode,
    cpu_agent_scope as _cpu_agent_scope,
    is_cpu_agent_active as _is_cpu_agent_active,
    low_rr_spec_abstain_shikii,
    extract_spec_tokens as _extract_spec_tokens,
    spec_token_matches_text as _spec_token_matches_text,
    attach_spec_coverage_diag as _attach_spec_coverage_diag,
    attach_cpu_agent_confidence_diag as _attach_cpu_agent_confidence_diag,
)
from brain_review_triage import (
    REVIEW_NOISE_TOPICS as _REVIEW_NOISE_TOPICS,
    REVIEW_NOISE_TYPES as _REVIEW_NOISE_TYPES,
    REVIEW_MECHANICAL_PREFIXES as _REVIEW_MECHANICAL_PREFIXES,
    l3_ready_event_ids as _l3_ready_event_ids,
    triage_review_row,
)
try:
    import stop_flag as _stop_flag  # 王命2026-09-08 第6弾3章 停止旗
except Exception:
    _stop_flag = None
from ai_availability import read_shared as ai_availability_payload, overlay as availability_overlay
# 2026-09-26 ai-12ef: 名義の**本体ID**。正本は roundtable_event._canon_name の1つだけ（表を写さない）。
#   roundtable_event は標準ライブラリしか import せず、共有脳を import しないので循環しない
#   （確かめた。session_start は元から関数の中で roundtable_event を import していた）。
#   記録(acks.jsonl・events)の生の名は書き換えない。**読む時・比べる時にだけ**本体へ揃える。
from roundtable_event import _canon_name as _hontai_mei


SCHEMA = "kingdom.roundtable.shared_brain.v2"
# 稼働中のコードの版(2026-08-28 ネオ・くくるの提案書 P0-3)。
#   2026-08-24、常駐プロセスが古いbrainコードを19時間抱えたまま応答していた。
#   「ファイルを直した」は「その版が動いている」ではない。読む側が自分で気づけるよう、
#   応答に版を載せる。データの版(snapshot.version)とは別物。
_CODE_DIR = Path(__file__).resolve().parent
# 出所(shussho)を導く純粋関数。procedural層の1件ごとに「誰が書いた物か」を
# パスだけから決定的に付ける(OWASP ASI06・Claude指示書 2026-09-21 12:44)。
# ★欄を保存せず毎回導く ―― ionalpha/flynn memory/guard の原文
#   "TrustOf is a pure function so retrieval and the write gate agree
#    without storing a redundant field" に合わせた(2026-09-21 ネオ ai-b6cc)。
if str(_CODE_DIR) not in sys.path:
    sys.path.insert(0, str(_CODE_DIR))
import brain_speed_cache as _speed_cache
from brain_speed_cache import (
    ProceduralFiles, redact_with_rules,
    freshness_cache_ttl as _freshness_cache_ttl,
    invalidate_freshness_cache as _invalidate_freshness_cache,
    freshness_payload_impl as _bsc_freshness_payload,
    render_context_impl as _bsc_render_context,
    audit_impl as _bsc_audit,
    main_impl as _bsc_main,
)
try:
    from shussho import michibiku as _shussho_michibiku
except Exception:      # 道具が無くても頭脳は止めない(札が付かないだけ)
    def _shussho_michibiku(path, honbun=None):
        return {"shussho": "oukoku_ai", "shinrai": 1,
                "gaibu_no_ato": False, "jikkou_shinrai": 1}


def _shussho_fuda(path: str, honbun: str = "") -> dict:
    """procedural の1行に足す出所の札。値は毎回パスから導く(保存しない)。"""
    v = _shussho_michibiku(path, honbun)
    return {
        "shussho": v["shussho"],
        "shinrai": v["shinrai"],
        "gaibu_no_ato": v["gaibu_no_ato"],
        "jikkou_shinrai": v["jikkou_shinrai"],
    }

_CODE_FILES = ("roundtable_brain.py", "roundtable_imi.py", "nirai_record_roles.py", "brain_bm25.py", "brain_review_triage.py", "brain_text_utils.py")
EPISODE_SCHEMA = "kingdom.roundtable.shared_brain.episode.v2"
# ack v3 (2026-08-31 ネオ): 単位を版ハッシュ→イベント読了位置(read_upto)へ。
# 理由(実測): 版はイベント1件ごとに変わるため「全員が最新版をack」は一瞬しか成立しない
# (Codeの14:45のackが17分後にcurrent:false)。読了位置は後続イベントで無効にならず、
# 状態は「未読が何件か」で出る。イベントログ界の確立形(consumer offset)と同じ。
ACK_SCHEMA = "kingdom.roundtable.shared_brain.ack.v3"
VERIFICATION_SCHEMA = "kingdom.roundtable.shared_brain.verification.v1"
# v2検証台帳(scripts/kenshou.py touroku_v2 が書く)。episode の trust へ繋ぐのは下の強い段だけ。
VERIFICATION_V2_SCHEMA = "kingdom.roundtable.shared_brain.verification.v2"
STRONG_V2_LEVELS = frozenset({"L3_reproduction", "L4_independent_reproduction"})
# v2強検証の却下の受け止め台帳(2026-09-27 ai-87bb 2回目)。v2台帳は他人が書く追記専用の正本で、
# 共有脳は1バイトも書き換えない。そこに「独立でない L3/L4 pass」が在ると、audit の error が永久に消えない
# (rt_task_contracts・学びのループの audit まで巻き添えで赤になる)。見た者が理由を添えてここへ1行書くと、
# その却下だけ warning へ回る。**trust は一切上げない**(昇格の判定はこの台帳を読まない)。
V2_KYAKKA_SCHEMA = "kingdom.roundtable.shared_brain.verification_v2_kyakka.v1"
# 受け止めてよい却下の理由。対象の変化・証拠の差し替えは受け止めさせない(改ざんの警報を黙らせない)。
V2_KYAKKA_RIYUU = frozenset({"not_independent"})
CORRECTION_SCHEMA = "kingdom.roundtable.shared_brain.correction.v1"
MAX_EVENT_CHARS = 6000
FRESHNESS_GRACE_SECONDS = 390
# バグ1の直し(2026-09-19 王命): 壁時計の age_seconds がこの秒数を超えたら
#   state を強制的に stale にする。写し自体の古さで判断する。
#   ブレイン更新は 5 分周期(brain-refresh.timer)。10 分止まったら明示stale。
FRESHNESS_STALE_AGE_SECONDS = 600
# ★2026-09-21 ネオ ai-0512（王命_20260919-0955 バグ7・王の指示「バグも一緒に片付ければいいよ」）
#   この名簿は「誰がどこまで円卓を読んだか」の表（ack_matrix）を作るためだけに使う。
#   **名簿と実体が食い違い、3席が永久に空欄のまま並んでいた。**
#
#   実測 2026-09-21 12:5x（既読の台帳 roundtable/brain/acks.jsonl と、直近30日の発言数）
#     ┌ 名簿に在るが **発言も既読も1件も無い** … くくるV5（発言12・既読なし）／
#     │   くくる(王国円卓つき)（発言0）／ Qwen3.8（発言0）  ← ★この3席が「空欄」の正体
#     └ **既読を記録しているのに名簿に無い** … くくる（発言248・既読 01789923716012478847）／
#         GPTくくる（既読 01789872420051286491）  ← 読んだ事が表に出てこない
#
#   ⚠️「分からない物を不明と出す」のは正しい（2026-09-05の直し）。だが
#   **実在しない席の不明が常に3つ並ぶと、本物の不明がその中に埋もれる。**
#   （今日直した警報の穴と同じ型 ―― 意味の無い常時の赤は、見る目を殺す）
# ★2026-09-26 ai-12ef: **本体単位で1席**に畳んだ。前は くくる が3席
#   （"ChatGPT" / "くくる" / "GPTくくる"）に割れ、同じ主体の未読が3つの数で並んでいた
#   （本番の acks.jsonl と正本で実測。2026-09-26 昼の調査で 11 / 52 / 2749、
#     同日17時の写しで 122 / 163 / 2860 ―― 3つとも同じ111件ずつ増えただけの同じ主体）。
#   どれが本当か誰にも決められない。直した後は1行・既読位置が一番先の物(=最小の未読)。
#   ChatGPT・GPTくくる は「表示名(過去の名乗り)」として、表の行の besshou に残る。
#   名寄せの正本は roundtable_event._canon_name（ここに別名を書かない）。
# ★2026-09-26 ネオ ai-5fa9 / 王命_20260926-1924: 現行名義を「くくるV7」にした。
#   名寄せの正本は roundtable_event._canon_name（ChatGPT/GPTくくる/kukuru/くくる → くくるV7）。
#   ここは「今いる本体」の一覧。過去の記録の生の名は書き換えない。
AI_PARTICIPANTS = (
    "くくるV7", "Code", "Codex", "Claude", "チム", "Gemini",
)
# 昔の名（外したが、消したのではない）。過去の記録にはこの名で残っている。
# ⚠️2026-09-26 ai-12ef: 「くくるV5 → くくる」と書いてあったのは**誤り**だった。
#   「くくるV5」は**チム系のローカルAI**が今も名乗る名（rt_adapter_kukuru_wake.py の AGENT。
#   正本の実測で 09-20〜09-26 も毎日 04:10 に発言。rt_adapter_chimu_wake.py は
#   「チム」「くくるV5」を**チム自身の前の発言**として扱う）。ChatGPT上のくくるとは別の主体。
#   名寄せ(_canon_name)には**入れない**。ここは「名簿から外した名」の控えでしかない。
AI_PARTICIPANTS_MUKASHI_NO_NA = (
    "くくる",                  # 2026-09-26まで現行だった名。今は くくるV7 へ寄せる（_BESSHOU）
    "くくるV5",                # チム系のローカルAIの名乗り（くくるV7≠くくるV5。束ねない）
    "くくる(王国円卓つき)",      # → 今は「くくる」（GPTのプロジェクト版）
    "Qwen3.8",                 # 王国のモデル。円卓では発言しない
)
EVIDENCE_PREFIXES = (
    "reports/", "checks/", "code_wisdom/checks/", "code_wisdom/sessions/", "status/", "records/",
)
TERMINAL_TYPES = {"complete", "blocked", "answer"}
STATE_TYPES = {"claim", "progress", "complete", "blocked"}
# 脳に載せる出来事の型。王命・質問・提案は実測の印が付かないので、型で拾う(2026-08-20)
LOAD_TYPES = STATE_TYPES | TERMINAL_TYPES | {"instruction", "question", "proposal"}
# 2026-09-09 ネオ ai-9598: 「道具の癖」を足した。
# 実測で分かった事 ―― 技能の棚 reports/道具の癖_*.md 19本のうち **18本は題に「使い方」が
# 入っていたから通っていただけ**で、`道具の癖_許可の窓が出る条件_20260907.md` だけが
# どの語にも当たらず **頭脳に載っていなかった＝呼ばれようがなかった**（索引には10回出るのに）。
# 技能の棚の目印は `gino_no_tsukawarekata.py` の SHIRUSHI = "道具の癖" なので、
# **棚の定義と、脳に載る条件を同じ物にする**（題の付け方で決まる形は必ず腐る）。
# 増えるのは実測で2件だけ（上記の技能1本＋王命追記1本）。
# ⚠️ 2026-08-20 に白名簿を外して 205→1,161件にした時は成績が下がったが、
#    その理由は「増やした956件の本文が900字しか索引に入っていなかった」事で、
#    今は本文がまるごと入る（MAX_BODY_CHARS）。同じ理由では起きない。それでも測る。
PROCEDURAL_WORDS = ("掟", "基準", "手順", "使い方", "運用", "憲法", "ループエンジニアリング",
                    "道具の癖")
# 各ヒントは、その正規表現が一致するために必要な文字列の候補。
_SECRET_BEARER_RULE = (re.compile(r"(?i)(bearer\s+)[A-Za-z0-9._~+\-/=]{12,}"), "bearer")
_SECRET_ASSIGNMENT_RULE = (
    re.compile(r"(?i)((?:api[_-]?key|token|password|passwd|pwd|secret)\s*[:=]\s*)[^\s,;]{8,}"),
    ("apikey", "api_key", "api-key", "token", "password", "passwd", "pwd", "secret"),
)
_SECRET_LITERAL_RULES = (
    (re.compile(r"\bsk-[A-Za-z0-9_-]{12,}\b"), ("sk-",)),
    # ここから下は mcp/server.py の sanitize(84行付近)から写した(2026-08-29 修理・バグ8)。
    # importすると循環になるので写しで持つ。素通しだったGitHub PAT/GitLab/HF/AWS鍵/JWT/PEMの栓。
    # server.pyの裸の32桁hexは、記録が正当に引用するcontent_sha256まで消すので写さない。
    (re.compile(r"\bgithub_pat_[A-Za-z0-9_]{10,}\b"), ("github_pat_",)),
    (re.compile(r"\bglpat-[A-Za-z0-9_-]{10,}\b"), ("glpat-",)),
    (re.compile(r"\bhf_[A-Za-z0-9]{10,}\b"), ("hf_",)),
    (re.compile(r"\bghp_[A-Za-z0-9_-]{10,}\b"), ("ghp_",)),
    (re.compile(r"\bgsk_[A-Za-z0-9_-]{10,}\b"), ("gsk_",)),
    (re.compile(r"\bAIza[A-Za-z0-9_-]{10,}\b"), ("AIza",)),
    (re.compile(r"\bxox[baprs]-[A-Za-z0-9-]{10,}\b"), ("xox",)),
    (re.compile(r"\b(?:AKIA|ASIA)[0-9A-Z]{16}\b"), ("AKIA", "ASIA")),
    (re.compile(r"\beyJ[A-Za-z0-9_-]{10,}\.eyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\b"), ("eyJ",)),
    (re.compile(r"-----BEGIN [A-Z0-9 ]*PRIVATE KEY-----.*?-----END [A-Z0-9 ]*PRIVATE KEY-----", re.DOTALL), ("PRIVATE KEY-----",)),
    # 2026-09-12 追加。口(roundtable_event.py)に在って共有脳に無かった栓を写した
    #   (実測: 16種の俵で共有脳だけが6種漏らしていた)。閉じ在りPEMの**後ろ**に置く。
    # ・ENDが欠けたPEM = 鍵の本体が素通りしていた。口は2026-09-08に塞いだが共有脳は残っていた
    (re.compile(r"-----BEGIN [A-Z0-9 ]*PRIVATE KEY-----[A-Za-z0-9+/=\s]{0,8000}"), ("PRIVATE KEY-----",)),
    # ・認証情報を含むURLは保管せず、形式だけを検出して伏せる。scheme は残す
    (re.compile(r"(?i)\b([a-z][a-z0-9+.\-]*://)[^\s:/@]+:[^\s:/@]+@"), ("@",)),
    # ⚠️ 口に在る「お客様のメール・電話・郵便番号」の栓は**写さない**。
    #   共有脳は events だけでなく reports/ と 共通メモリ/技能/ を**直接**食べており、
    #   そこには**王の店自身の連絡先**が入っている(実測 2026-09-12:
    #   技能 mise-no-tadashii-atai-de-kaku/scripts/mise_no_atai_wo_shiraberu.py 他)。
    #   伏せるとチムが署名・納品案内・見積を書けなくなる
    #   (チムの署名未達は2026-09-04からの未解決の宿題。悪化させない)。
    #   お客様の個人情報は**保存する側(口)で既に伏せている**ので、二重に伏せる必要は無い。
    # 和文パスワードはserver.pyにも無かったので新設(バグ8の指摘対象)
    (re.compile(r"((?:パスワード|暗証番号|合言葉)\s*[:：=は]\s*)[^\s、。]{4,}"), ("パスワード", "暗証番号", "合言葉")),
)
SECRET_RES = (_SECRET_BEARER_RULE[0], _SECRET_ASSIGNMENT_RULE[0],
              *(pattern for pattern, _ in _SECRET_LITERAL_RULES))


# 行頭の【要点】だけを拾う。文中の「先頭に【要点】1行と書く」まで要点と誤読していた(2026-08-20 実測)
YOUTEN_MARK = re.compile(r"^[\s*>・-]*【要点】\s*(.+)", re.M)
SANRYO_BLOCK = re.compile(r"<!--\s*3行要約:begin\s*-->(.*?)<!--\s*3行要約:end\s*-->", re.S)
# 要約ブロックの直後の生成日の行(wadai_sanryou_ireru.py が書く)・話題mdの記録の見出し・要約の現況2行
SANRYO_STAMP = re.compile(r"\s*<!--\s*3行要約の生成:\s*(\d{4})-(\d{2})-(\d{2})")
WADAI_KIROKU_MIDASHI = re.compile(r"^## \[(\d{8}) \d{2}:\d{2}\]", re.M)
SANRYO_GENJOU_GYO = re.compile(r"^[ \t]*(?:今どこ|次の一手)[:：][^\n]*\n?", re.M)
HEAD_CACHE: dict[tuple[str, int, int, int], tuple[str, bool]] = {}
# 2026-09-12 くくる監査④: 同一秒・同サイズ編集で古い本文が刺さる穴を避けるため
# キーに mtime_ns(ナノ秒)を加え、常駐でも溢れないよう LRU 淘汰する。
MAX_HEAD_CACHE_ENTRIES = 5000


def now_iso() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


def sanryo_koshi_kesu(text: str, full: str | None = None) -> str:
    """3行要約が、同じ話題の最新の記録より古い時、今どこ・次の一手の2行を外して注記に替える(2026-10-31 world-1)。

    話題mdの3行要約は生成日の行を持つが、記録は末尾へ足され続け、要約は作り直されない。
    実測: 70話題すべての生成日が 2026-08-20 で、27話題はそれより後の記録を持ち(遅れ中央値46日・最大72日)、
    「実装停止・以降の追記なし」が検索の summary/snippet に現況として出ていた。
    生成日より後の日付の記録が在る時だけ働く。生成日の行が無い・記録が無い・同じ日までなら text をそのまま返す。
    text は full(ファイル全文)の先頭部分でよい(位置が揃う)。記録の日付は全文でしか分からない。
    """
    block = SANRYO_BLOCK.search(text, 0, 8000)
    if not block:
        return text
    full = text if full is None else full
    stamp = SANRYO_STAMP.match(full, block.end())
    if not stamp:
        return text
    sakusei = "".join(stamp.groups())
    saishin = max(WADAI_KIROKU_MIDASHI.findall(full, stamp.end()), default="")
    if saishin <= sakusei:
        return text
    nokori = SANRYO_GENJOU_GYO.sub("", block.group(1)).rstrip("\n")
    chuki = (f"【3行要約は{'-'.join(stamp.groups())}時点で古いため外した。"
             f"この話題には{saishin[:4]}-{saishin[4:6]}-{saishin[6:]}まで記録が続く。現況は末尾の日付つき記録で読む】")
    return text[:block.start(1)] + nokori + "\n" + chuki + "\n" + text[block.end(1):]


def record_head(root: Path, rel_path: str, limit: int = 900) -> tuple[str, bool]:
    """索引された記録の中身から要点を取り出す。3行要約→【要点】→本文先頭の順(2026-08-20 ネオ)。

    戻りの2つ目は「3行要約ブロックから取れたか」。話題の見出しは検索で少し上へ出す。
    ただし最新の記録より古い3行要約は False(今どこ・次の一手を外した注記になる・sanryo_koshi_kesu)。

    なぜ要るか: procedural層はタイトルとパスしか持たず、本文で検索できず切れ端も空だった。
    ファイル先頭8000字までしか読まない。同じmtime/サイズならキャッシュを使う。
    """
    target = root / rel_path
    try:
        stat = target.stat()
    except OSError:
        return "", False
    # 2026-09-12 くくる監査④: mtime_ns で同一秒・同サイズ編集を弾く
    key = (str(target.resolve()), stat.st_size, int(stat.st_mtime_ns), limit)
    result, _ = _record_head_cached(target, key, limit)
    return result


def _record_head_cached(target, key, limit, ready=None):
    cached = HEAD_CACHE.get(key)
    if cached is not None:
        return cached, False
    if ready is not None:
        HEAD_CACHE[key] = ready
        if len(HEAD_CACHE) > MAX_HEAD_CACHE_ENTRIES:
            HEAD_CACHE.pop(next(iter(HEAD_CACHE)))
        return ready, True
    stable = True
    try:
        with target.open("r", encoding="utf-8", errors="replace") as handle:
            raw = handle.read(8000)
    except OSError:
        raw, stable = "", False
    koshi = False
    if SANRYO_BLOCK.search(raw):
        # 要約より後の記録が在るかは先頭8000字では分からない(記録は末尾へ足される)ので全文を見る
        try:
            kesu = sanryo_koshi_kesu(raw, target.read_text(encoding="utf-8", errors="replace"))
        except OSError:
            kesu, stable = raw, False
        koshi, raw = kesu != raw, kesu
    block = SANRYO_BLOCK.search(raw)
    mark = YOUTEN_MARK.search(raw[:2000])
    if block:
        picked = block.group(1)
    elif mark:
        picked = mark.group(1)
    else:
        body = []
        for line in raw.splitlines():
            line = line.strip()
            if not line or line.startswith(("#", "---", "<!--", "```", "|", "* * *")):
                continue
            body.append(line)
            if sum(len(part) for part in body) >= limit:
                break
        picked = " ".join(body)
    # 行内の余分な空白だけ潰し、行の切れ目は残す(3行要約を3行のまま渡すため)
    tidy = [" ".join(line.split()) for line in redact(picked).splitlines()]
    head = "\n".join(line for line in tidy if line)[:limit]
    HEAD_CACHE[key] = (head, bool(block) and not koshi)
    if len(HEAD_CACHE) > MAX_HEAD_CACHE_ENTRIES:
        HEAD_CACHE.pop(next(iter(HEAD_CACHE)))  # 古い挿入から捨てる (insertion order)
    return HEAD_CACHE[key], stable


BODY_CACHE: dict[tuple[str, int, int], str] = {}
MAX_BODY_CACHE_ENTRIES = 5000
MAX_BODY_CHARS = 24000     # 1記録の本文の上限。bge-m3のctx8192tokenを超える分は刻んで使う


class RecordBodyChangedError(OSError):
    """The selected file changed while its body was being read."""

def _fd_stat_kagi(fd: int):
    st = os.fstat(fd)
    return ((st.st_dev, st.st_ino, st.st_size, st.st_mtime_ns, st.st_ctime_ns, st.st_mode),
            max(st.st_mtime_ns, st.st_ctime_ns))

def record_body(root: Path, rel_path: str, *, strict: bool = False,
                _observed_fingerprint: tuple | None = None,
                _opened_fd: int | None = None) -> str:
    """索引された記録の**本文まるごと**を返す(2026-08-20 ネオ)。

    なぜ要るか(実測): 今まで procedural層は record_head の900字しか索引に入っておらず、
    載せた313ファイルの実バイト2,804KBに対し索引に入ったのは271KB=**9.7%**だった。
    雑談.md(134,490バイト)は366バイト=0.3%しか入っていない。
    「本文で検索できるようにした」という注釈が、実態と9割ずれていた。
    """
    # 公開fetchはstrictで取得失敗を受け取る。既存の索引生成は空文字の
    # best-effort契約を保つが、失敗した空文字をキャッシュには保存しない。
    target = root / rel_path
    try:
        # strict fetchは入口で同じ実体をstat済み。その観測を再利用し、
        # キャッシュ命中時の重複statを増やさない。出口では再照合する。
        if _opened_fd is not None:
            fingerprint, _ = _fd_stat_kagi(_opened_fd)
        elif _observed_fingerprint is not None:
            fingerprint = _observed_fingerprint
        else:
            fingerprint, _ = _stat_kagi(target)
    except OSError:
        if strict:
            raise
        return ""
    # hashcacheと同じdev/inode/size/mtime/ctime/modeの指紋を使う。
    # パスも絶対名で持ち、別ROOTの同名資料を混ぜない。毎回の全本文hashは不要。
    # Public FD reads must not consume entries populated by legacy path readers.
    key = ("fd" if _opened_fd is not None else "path", str(target.absolute()), fingerprint)
    cached = BODY_CACHE.get(key)
    if cached is not None:
        return cached
    try:
        if _opened_fd is None:
            # 索引用の読み。最新の記録より古い3行要約の現況2行は外す(公開fetchのFD読みは本文のまま)
            full = target.read_text(encoding="utf-8", errors="replace")
            raw = sanryo_koshi_kesu(full[:MAX_BODY_CHARS * 2], full)
            after_read, _ = _stat_kagi(target)
        else:
            # The caller owns this descriptor, opened below its trusted ROOT.
            # No pathname is reopened; closefd=False leaves closure to its owner.
            with os.fdopen(_opened_fd, "r", encoding="utf-8", errors="replace", closefd=False) as stream:
                raw = stream.read(MAX_BODY_CHARS * 2)
            after_read, _ = _fd_stat_kagi(_opened_fd)
        if after_read != fingerprint:
            raise RecordBodyChangedError("record changed during read")
    except OSError:
        if strict:
            raise
        return ""
    keep = []
    for line in raw.splitlines():
        line = line.strip()
        if not line or line.startswith(("<!--", "```")):
            continue                       # 目印のコメントと囲みは検索の役に立たない
        keep.append(line)
    body = redact(" ".join(keep))[:MAX_BODY_CHARS]
    BODY_CACHE[key] = body
    if len(BODY_CACHE) > MAX_BODY_CACHE_ENTRIES:
        BODY_CACHE.pop(next(iter(BODY_CACHE)))  # 古い挿入から捨てる
    return body


def sha_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


# 2026-10-03 ai-3170(札 6246・候補): 中身の指紋を「変わっていない物は覚えて使い回す」。
#   実測: 頭脳の検索1回のうち 4.5〜5.3秒が鮮度の確かめ(freshness_payload)で、
#   107MB の記録総合索引を毎回読み直し・同じファイルの sha256 を毎回取り直していた。
#   指紋は**中身(sha256)のまま**(08-29 の修理=空書きで版が変わらない を守る)。覚えるのは
#   (inode, 大きさ, 更新時刻ns) が同じ間だけ。更新から2秒以内の物は覚えない(git の racy 対策と同じ)。
_SHA_OBOE: dict = {}
_OBOE_YOYUU_NS = 2_000_000_000


def _stat_kagi(path: Path):
    # 2026-10-03 ai-6c56(札 c477): git の racy-git と同じ物を鍵にする(git-scm.com/docs/racy-git:
    #   mtime と ctime・inode・dev・大きさ・mode を比べる)。ctime は利用者が戻せないので、
    #   rsync -a / cp -p のように mtime を保ったまま中身を差し替えた時も鍵が変わる。
    #   「新しすぎて覚えない」判定は mtime と ctime の新しい方で見る。
    st = path.stat()
    return ((st.st_dev, st.st_ino, st.st_size, st.st_mtime_ns, st.st_ctime_ns, st.st_mode),
            max(st.st_mtime_ns, st.st_ctime_ns))


def sha_file(path: Path) -> str:
    try:
        kagi, mtime_ns = _stat_kagi(path)
    except OSError:
        kagi, mtime_ns = None, 0
    if kagi is not None:
        hit = _SHA_OBOE.get(str(path))
        if hit and hit[0] == kagi:
            return hit[1]
    ima = _jikan_ai3170.time_ns()          # ★c477 反証1回目(確かめられない2): 余裕は stat の直後の時刻で測る
    got = _sha_file_yomu(path)
    if kagi is not None and ima - mtime_ns > _OBOE_YOYUU_NS and _onaji_mama(path, kagi):
        _SHA_OBOE[str(path)] = (kagi, got)
    return got


def _onaji_mama(path: Path, kagi) -> bool:
    """★2026-10-03 ai-6c56(札 c477 反証1回目): 読んだ後にもう一度 stat し、読む前の鍵と同じ時だけ覚える
    (読んでいる途中に書き換わった物を、古い鍵で覚えない。git の racy-git が中身で確かめ直すのと同じ向き)。"""
    try:
        return _stat_kagi(path)[0] == kagi
    except OSError:
        return False


def _sha_file_yomu(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


_ORIGINAL_SECRET_RES = SECRET_RES


def redact(text: str) -> str:
    return redact_with_rules(text, SECRET_RES, _ORIGINAL_SECRET_RES, _SECRET_BEARER_RULE,
                             _SECRET_ASSIGNMENT_RULE, _SECRET_LITERAL_RULES)


def atomic_write(path: Path, text: str, mode: int = 0o664) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(tmp_name, mode)
        os.replace(tmp_name, path)
        directory_fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        if os.path.exists(tmp_name):
            os.unlink(tmp_name)


def append_jsonl(path: Path, rows: list[dict]) -> None:
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        # 末尾が改行で終わっていない(書込み途中で切れた行)なら、先に改行を足して
        # 切れた行を独立した壊れ行として隔離する。足さないと次の有効な行が連結されて失われる。
        try:
            with path.open("rb") as tail_handle:
                tail_handle.seek(0, os.SEEK_END)
                if tail_handle.tell() > 0:
                    tail_handle.seek(-1, os.SEEK_END)
                    if tail_handle.read(1) != b"\n":
                        handle.write("\n")
        except OSError:
            pass
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")
        handle.flush()
        os.fsync(handle.fileno())
        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def read_json(path: Path, default):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return default


def read_jsonl(path: Path) -> list[dict]:
    rows = []
    if not path.is_file():
        return rows
    with path.open(encoding="utf-8", errors="replace") as handle:
        for line_number, line in enumerate(handle, 1):
            line = line.strip()
            if not line:
                continue
            try:
                value = json.loads(line)
            except json.JSONDecodeError:
                value = {"_invalid": True, "_line": line_number, "_raw_sha256": sha_text(line)}
            if isinstance(value, dict):
                rows.append(value)
    return rows


def event_files(root: Path) -> list[Path]:
    return sorted((root / "roundtable" / "events").glob("events_*.jsonl"))


def _event_time_key(item: dict) -> tuple:
    """Compare instants, including mixed offsets; legacy naive dates use JST.

    Missing/invalid times precede valid events and retain deterministic text/id order.
    """
    raw = str(item.get("timestamp") or "")
    event_id = str(item.get("event_id") or "")
    try:
        when = datetime.fromisoformat(raw)
        if when.tzinfo is None:
            when = when.replace(tzinfo=timezone(timedelta(hours=9)))
        return (1, when.astimezone(timezone.utc), event_id)
    except (ValueError, TypeError, OverflowError):
        return (0, raw, event_id)


def load_events(root: Path) -> list[dict]:
    # Full bytes checked every time; only decoded event objects are reused.
    from kingdom_read_acceleration import brain_events
    rows = brain_events(root)
    # 2026-10-17 ネオ ai-2b30: F2_F5 の時差の直しを 3409 へ当て直し。brain_events は文字の時刻で並べるので、
    # 時差つきの時刻を同じ瞬間として比べる鍵で並べ直す（新しいリストなので並べ替えても覚えは汚れない）。
    rows.sort(key=_event_time_key)
    return rows


_EVENTS_META_LOCK = threading.RLock()
_EVENTS_META_OBOE: dict[str, tuple[tuple, tuple]] = {}


def load_events_meta(root: Path) -> list:
    """freshness/loop_alignment 向けの軽量メタ読込(event_id, timestamp)。
    全33,700件のdict深い写し(_json_copy)を省きつつ、_event_time_key 順と内容検証を厳密一致させる。"""
    from kingdom_read_acceleration import brain_events_meta_with_sig
    sig, rows = brain_events_meta_with_sig(root)
    key = str(Path(root).resolve())
    with _EVENTS_META_LOCK:
        hit = _EVENTS_META_OBOE.get(key)
        if hit is not None and hit[0] == sig:
            return list(hit[1])
    rows.sort(key=_event_time_key)
    frozen = tuple(rows)
    with _EVENTS_META_LOCK:
        if len(_EVENTS_META_OBOE) >= 16:
            _EVENTS_META_OBOE.clear()
        _EVENTS_META_OBOE[key] = (sig, frozen)
    return list(frozen)


_EVENTS_RO_LOCK = threading.RLock()
_EVENTS_RO_OBOE: dict[str, tuple[tuple, tuple]] = {}


def load_events_readonly(root: Path) -> list:
    """読み取り専用経路(session_start/audit等)向けの不変イベント読込。
    全イベントのdict/list深い写し(_json_copy)と毎回ソートを省きつつ、
    isinstance(dict/list)・json.dumps・_event_time_key順・内容検証を厳密一致させる。"""
    from kingdom_read_acceleration import brain_events_readonly_with_sig
    sig, rows = brain_events_readonly_with_sig(root)
    key = str(Path(root).resolve())
    with _EVENTS_RO_LOCK:
        hit = _EVENTS_RO_OBOE.get(key)
        if hit is not None and hit[0] == sig:
            return list(hit[1])
    rows.sort(key=_event_time_key)
    frozen = tuple(rows)
    with _EVENTS_RO_LOCK:
        if len(_EVENTS_RO_OBOE) >= 16:
            _EVENTS_RO_OBOE.clear()
        _EVENTS_RO_OBOE[key] = (sig, frozen)
    return list(frozen)


def kowareta_gyou(root: Path) -> list[dict]:
    """円卓正本の壊れた行を拾って返す。2026-09-01 修理(バグ249)。

    read_jsonl は壊れ行に _invalid の印を付けるが、load_events はそれを黙って捨てる。
    捨てるのは正しい(壊れた行を脳に食わせる方が害が大きい)。だが**数が消える**のが問題で、
    正本から何行落ちたのか audit にも counts にも出なかった。ここで数えて見えるようにする。
    """
    try:
        from kingdom_read_acceleration import brain_events_invalid
        return brain_events_invalid(root)
    except Exception:
        pass
    out = []
    for path in event_files(root):
        for row in read_jsonl(path):
            if row.get("_invalid"):
                out.append({"file": str(path.relative_to(root)),
                            "line": row.get("_line"),
                            "sha256": row.get("_raw_sha256")})
    return out


def source_manifest(root: Path) -> dict[str, str]:
    verified_files = sorted((root / "data" / "open-webui" / "chimu_record_memory" / "verified").glob("*.md"))
    candidates = event_files(root) + verified_files + [
        # 生成ロジックが変わった場合も、入力データが同じだからと再生成を
        # 省略しない。配備先のスクリプト自体を版の構成要素に含める。
        root / "scripts" / "roundtable_brain.py",
        root / "roundtable" / "resolutions.jsonl",
        root / "reports" / "索引" / "_記録総合索引.json",
        root / "data" / "open-webui" / "chimu_record_memory" / "state.json",
        root / "roundtable" / "brain" / "verifications.jsonl",
        # v2検証台帳(2026-09-27 ai-87bb・王命_20260927-0426)。L3/L4 の強検証が trust を上げる。
        # ⚠️くくるの隔離実測で分かった配線漏れ: これを版の構成要素に入れないと、
        #   v2台帳に行が足されても refresh が unchanged で素通りし、昇格も降格も起きない。
        root / "roundtable" / "brain" / "verifications_v2.jsonl",
        # 別イベント・手順書を「現在は置換済み」と結ぶ追記専用台帳。
        # 履歴そのものは消さず、現在の問いでだけ新しい訂正を優先する。
        root / "roundtable" / "brain" / "corrections.jsonl",
        # 同族(名指しされていないが同じ主張を書いた投稿)の審議台帳。
        # ⚠️2026-09-08 実測: これを版の構成要素に入れ忘れると、審議して「認める」と書いても
        # refresh が unchanged で素通りし、検索の印が⚠️のまま変わらない(黙って効かない)。
        root / "roundtable" / "brain" / "doukei.jsonl",
        # 要点1行の外付け台帳(2026-08-16 ネオ)。これが変わっても再生成する
        root / "roundtable" / "brain" / "youten.jsonl",
        # 画像・動画台帳。event本文が同じでも添付が追加されたら脳を更新する。
        root / "roundtable" / "media" / "manifest.jsonl",
        # 道具台帳(段A・王命2026-09-04)。行が追加されたら procedural を作り直す
        root / "roundtable" / "workbench" / "code_registry.jsonl",
        # 道具の索引(2026-09-09 ネオ)。docstringが直れば説明も直るので、指紋に含めて追随させる
        root / "roundtable" / "brain" / "dougu.jsonl",
    ]
    manifest = {}
    for path in candidates:
        if path.is_file():
            manifest[str(path.relative_to(root))] = sha_file(path)
    # procedural層は記録の本文を読む。話題mdに3行要約を入れても版が変わらず、
    # 脳が作り直されないまま「unchanged」で素通りしていた(2026-08-20 実測・ネオ)
    manifest["@procedural_bodies"] = procedural_body_version(root)
    return manifest


GOMI_MARKERS = ("backups/", ".bak", "trainers/", "/log", "log/", ".jsonl",
                "__pycache__", "/venv/", "node_modules", "/古い", "旧_")
GOMI_CATEGORY = {"08_学習データ", "09_履歴保管"}
MIN_BYTES, MAX_BYTES = 200, 300_000


def procedural_target(path: str, record: dict) -> bool:
    """procedural層に載せる記録か。載せる物と本文の指紋を、この1箇所で決める。

    2026-08-20: 7語の白名簿(掟・基準・手順・使い方・運用・憲法・ループエンジニアリング)を**廃止**した。
    実測: 白名簿では記録索引2,864件のうち205件しか通らず、ネオの記憶191件は2件しか脳に載っていなかった。
    「王の問いの答えが王国に在るのに脳が引けない」事故の一次原因がここ1箇所だった。
    代わりに**ゴミを除く**方式にする。白名簿は「知っている物しか入れない」ので、
    知らない物を探すための脳とは相性が逆だった。

    2026-08-20(同日・戻した): 白名簿を外して205→1,161件にしたら**成績が下がった**。
    実測 R@3 0.467→0.417 / MRR 0.418→0.382 / 無い物正答率 0.92→0.80(良化3問・悪化8問)。
    理由は明らかで、増やした956件の**本文が900字しか索引に入っていない**ため、
    薄い記録がノイズとして増えただけだった。**先に本文を入れる。関所を外すのはその後**。
    この順序を守るために、いったん白名簿へ戻す。
    """
    size = record.get("bytes")
    if type(path) is str and type(size) is int:
        if not MIN_BYTES <= size <= MAX_BYTES:
            return False
        category = record.get("category")
        if category is None or type(category) is str:
            if category in GOMI_CATEGORY:
                return False
            if category in _PROCEDURAL_ALWAYS:
                return not _PROCEDURAL_EXCLUDE_RE.search(path)
            title = record.get("title")
            if title is None or type(title) is str:
                # All keywords are non-ASCII and contain no spaces. The fallback
                # stem is a substring of path, and no match crosses the separator.
                if (path.isascii() or not _PROCEDURAL_WORD_RE.search(path)) and (
                        not title or title.isascii() or not _PROCEDURAL_WORD_RE.search(title)):
                    return False
                return not _PROCEDURAL_EXCLUDE_RE.search(path)
    # Keep the historical conversion and exclude order for atypical catalog rows.
    if any(marker in path for marker in COPY_MARKERS + GOMI_MARKERS):
        return False
    if str(record.get("category") or "") in GOMI_CATEGORY:
        return False
    size = int(record.get("bytes") or 0)
    if not (MIN_BYTES <= size <= MAX_BYTES):
        return False
    if str(record.get("category") or "") in {"00_共通メモリ", "01_正式指示"}:
        return True
    joined = path + " " + str(record.get("title") or Path(path).stem)
    return any(word in joined for word in PROCEDURAL_WORDS)


_TARGET_PATHS_OBOE = {}
_TARGET_PATHS_LOCK = threading.RLock()


def _procedural_target_paths(root: Path) -> tuple[str, ...]:
    # Cache the selection, never the current body hashes or change times.
    path = root / "reports" / "索引" / "_記録総合索引.json"
    try:
        fingerprint = sha_file(path)
    except OSError:
        fingerprint = None
    key = (str(root.resolve()), fingerprint, id(procedural_target))
    with _TARGET_PATHS_LOCK:
        if fingerprint and key in _TARGET_PATHS_OBOE:
            return _TARGET_PATHS_OBOE[key]
        catalog, data = report_index(root)
        paths = tuple(p for p, row in sorted(catalog.items()) if procedural_target(p, row))
        if fingerprint and isinstance(data.get("records"), list) and sha_file(path) == fingerprint:
            _TARGET_PATHS_OBOE.clear()
            _TARGET_PATHS_OBOE[key] = paths
        return paths


def procedural_body_version(root: Path) -> str:
    """procedural層が本文を読むファイルの指紋。内容のsha256で見る。

    2026-08-29 修理(バグ1脳側): mtime指紋はspec-refreshの空書き(同内容の書き直し)で
    毎回変わり、5分ごとに脳を全再構築させていた。内容が同じなら版も同じにする。
    """
    parts = []
    for path in _procedural_target_paths(root):
        try:
            digest = sha_file(root / path)
        except OSError:
            continue
        parts.append(f"{path}:{digest}")
    # 道具台帳(段A・王命2026-09-04)。procedural に載る行の元なので指紋に含める
    daichou = root / "roundtable" / "workbench" / "code_registry.jsonl"
    if daichou.is_file():
        try:
            parts.append("roundtable/workbench/code_registry.jsonl:" + sha_file(daichou))
        except OSError:
            pass
    return sha_text("\n".join(parts))


def current_source_version(root: Path) -> tuple[str, dict[str, str]]:
    manifest = source_manifest(root)
    encoded = json.dumps(
        {"schema": SCHEMA, "sources": manifest},
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return sha_text(encoded), manifest


def evidence_target(root: Path, evidence_path: str) -> Path:
    requested = Path(evidence_path)
    if requested.is_absolute() or ".." in requested.parts:
        raise RuntimeError("evidence path must be relative and cannot contain traversal")
    normalized = requested.as_posix()
    if not normalized or not any(normalized.startswith(prefix) for prefix in EVIDENCE_PREFIXES):
        raise RuntimeError("evidence path must be under reports, checks, code_wisdom, status, or records")
    unresolved = root / normalized
    if unresolved.is_symlink():
        raise RuntimeError("evidence symlink is not allowed")
    target = unresolved.resolve()
    try:
        target.relative_to(root.resolve())
    except ValueError as exc:
        raise RuntimeError("evidence path escapes root") from exc
    if not target.is_file():
        raise RuntimeError("evidence file not found")
    return target


def independent_verifier(sender: str, verifier: str) -> bool:
    """別名での自己検証を防ぐ。記録の生名は保ち、正本の本体IDで比べる。"""
    sender_id, verifier_id = _hontai_mei(sender), _hontai_mei(verifier)
    return bool(sender_id and verifier_id and sender_id != verifier_id)


def inspect_verifications(root: Path, events: list[dict] | None = None) -> tuple[list[dict], list[str]]:
    event_map = {
        str(event.get("event_id")): event
        for event in (events if events is not None else load_events(root))
        if event.get("event_id")
    }
    valid = []
    errors = []
    seen = set()
    for line_number, row in enumerate(read_jsonl(root / "roundtable" / "brain" / "verifications.jsonl"), 1):
        verification_id = str(row.get("verification_id") or "")
        event_id = str(row.get("event_id") or "")
        verifier = str(row.get("verifier") or "")
        event = event_map.get(event_id)
        prefix = f"verification:{line_number}"
        if row.get("schema") != VERIFICATION_SCHEMA:
            errors.append(f"{prefix}:schema")
        if not verification_id or verification_id in seen:
            errors.append(f"{prefix}:duplicate_or_missing_id")
        seen.add(verification_id)
        if event is None:
            errors.append(f"{prefix}:event_missing")
        elif not kiroku_to_dokuritsu(event, verifier):
            errors.append(f"{prefix}:not_independent")
        try:
            target = evidence_target(root, str(row.get("evidence_path") or ""))
            expected = str(row.get("evidence_sha256") or "")
            if len(expected) != 64 or sha_file(target) != expected:
                errors.append(f"{prefix}:evidence_hash")
        except RuntimeError:
            errors.append(f"{prefix}:evidence_path")
        if not any(item.startswith(prefix + ":") for item in errors):
            valid.append(row)
    return valid, errors


# ── v2検証台帳の「強検証」だけを episode の trust へ繋ぐ ─────────────────────────
#   ★2026-09-27 ai-87bb / 王命_20260927-0426（王「共有脳の検証済みepisode改善できるのかな？」
#     「進めていってくれ」・くくるの隔離候補 thread 01790450808634318796-f2650380）
#   実測(2026-09-27 04:17): verifications_v2.jsonl 94行 / 対象68 / pass対象67 のうち、
#     episode の trust へ届いていたのは 0件。verified_event_ids が v1 の4行しか読んでいなかった。
#   ★上げてよいのは L3/L4 の pass だけ。L2 は「証拠ファイルの照合」であって独立再現ではない。
#     partial / fail / inconclusive も上げない。**件数を目標にして門を緩めない。**
#   ★世界の型 = SLSA Verification Summary Attestation の How to verify:
#     subject の指紋が今の実物と一致する / 結果が PASSED / 段が期待の段を含む / 検証者が期待の主体
#     ―― を**読む側が全部確かめ直す**。書いた側の申告(result=pass)だけを信じない。
#     出典 https://slsa.dev/spec/v1.1/verification_summary （How to verify の節）
#   ★v2台帳は**読むだけ**。1バイトも書き換えない。
#   ★2026-09-27 ai-87bb 2回目（反証役の指摘・統括の決め）:
#     1回目は差出人・作者を**名前でしか比べず**、投稿した窓(event の mado)が自分の投稿を
#     検証しても昇格した。本番の fc7beafc がそれ（event sender=Code・mado=ai-ccd8、
#     verifier『qwen3.8:27b-ud-q4(手順を書いた)+ai-ccd8(関を通して回した)』・行自身の
#     independence_breakdown も execution=0）。統括の決め: **検証者の主体に作者の窓が
#     1つでも入っていたら独立と数えない。**区切りも + ＋ だけだと『Code、Qwen』
#     『Code & Qwen』『Code/ai-ccd8』が素通りしていた。大文字小文字(code/Code)も別人扱いだった。
#   ⚠️ Code の投稿を**別の** Code の窓が検証した行は、今も独立と数える(家系まで分けるかは王の判断待ち)。
_V2_KUGIRI = re.compile(r"[+＋、,，&＆/／・|｜;；]")
# 窓IDの形(ai-ccd8 / ai-runtime / codex-01a0ddd7)。括弧の役割書きの中まで探す相手を決めるのに使う。
_MADO_KATACHI = re.compile(r"^[A-Za-z]+-[0-9A-Za-z][0-9A-Za-z-]*$")


def _v2_shutai(name: str) -> list[str]:
    """検証者名・作者名を主体ごとに割る。

    実物の検証者名は『qwen3.8:27b-ud-q4(手順を書いた)+ai-ccd8(関を通して回した)』のように
    複数の主体を記号で繋ぎ、役割を括弧で添えている。括弧の中は役割の説明なので落とし、
    + ＋ 、 , ， & ＆ / ／ ・ | ｜ ; ； で割った1つずつを正本名(_hontai_mei)で比べる。
    空白では割らない（『Claude Code』を Claude と Code の2人にしない）。
    """
    s = re.sub(r"[（(][^（()）]*[)）]", "", str(name or ""))
    return [part.strip() for part in _V2_KUGIRI.split(s) if part.strip()]


def _v2_onaji_shutai(a: str, b: str) -> bool:
    """正本名で寄せたうえで大文字小文字を無視して同じ主体か。空は同じと見ない。"""
    ha = _hontai_mei(str(a or "").strip()).strip().casefold()
    hb = _hontai_mei(str(b or "").strip()).strip().casefold()
    return bool(ha) and ha == hb


def _v2_mado_ga_kaite_aru(mado: str, text: str) -> bool:
    """窓ID が検証者名のどこか（括弧の役割書きの中も）に1語として出るか。

    『qwen(手順)(ai-ccd8 が回した)』のように、回した窓が役割書きの中にだけ書かれる形も拾う。
    前後が英数字・-・_ なら別の語(ai-x1 と ai-x10 は別の窓)。
    """
    mado = str(mado or "").strip()
    if not mado:
        return False
    return re.search(r"(?<![0-9A-Za-z_-])" + re.escape(mado) + r"(?![0-9A-Za-z_-])",
                     str(text or ""), re.IGNORECASE) is not None


def v2_verifier_independent(sender: str, author: str, verifier: str,
                            mado: str = "", author_window: str = "") -> bool:
    """v2の検証者が、記録の差出人・作者・**投稿した窓**のどれとも別の主体か。

    **検証者の主体が1つでも差出人/作者/窓と同じなら独立ではない**（『Code+Qwen』で
    Code の記録を検証した事にさせない。『qwen(手順)+ai-ccd8(回した)』で ai-ccd8 の投稿を
    検証した事にさせない）。作者・差出人が空の行は比べられないので独立と認めない。
    mado = 円卓の出来事の mado（投稿した窓）。author_window = v2 行の author_window（在れば）。
    名寄せは roundtable_event._canon_name の1本だけを使う（表を写さない）。
    """
    parts = _v2_shutai(verifier)
    authors = _v2_shutai(author)
    if not parts or not authors or not str(sender or "").strip():
        return False
    windows = _v2_shutai(mado) + _v2_shutai(author_window)
    motogawa = [str(sender)] + authors + windows
    for part in parts:
        if any(_v2_onaji_shutai(part, moto) for moto in motogawa):
            return False
    for mado_id in windows + [a for a in authors if _MADO_KATACHI.match(a)]:
        if _v2_mado_ga_kaite_aru(mado_id, verifier):
            return False
    return True


def kiroku_to_dokuritsu(event: dict, verifier: str) -> bool:
    """v1 の検証者が、記録の差出人とも投稿した窓とも別の主体か(v2 と同じ判定を1本で使う)。"""
    sender = str(event.get("sender") or "")
    return v2_verifier_independent(sender, sender, verifier, mado=str(event.get("mado") or ""))


def episode_content(event: dict) -> str:
    """episode が保存する本文(伏字→上限で切る)。target_hash の照合もここを通す(計算を2箇所に置かない)。"""
    return redact(str(event.get("content") or ""))[:MAX_EVENT_CHARS]


def inspect_strong_verifications_v2(root: Path, events: list[dict] | None = None) -> tuple[list[dict], list[str]]:
    """v2台帳のうち、episode の trust を上げてよい『強検証』の行だけを返す。

    候補 = schema v2 かつ target_type=claim かつ L3/L4 かつ result=pass の行。
    候補が1つでも条件を欠けば昇格させず、理由を errors に積む(audit がそのまま error に出す)。
      target_id が今の円卓に在る / target_hash が今の episode 本文の指紋と一致 /
      stale_if_target_changes=true / 検証者が差出人とも作者とも別主体 /
      evidence_refs が1件以上・全部が証拠の許可範囲の中で実物の sha256 と一致
    候補でない行(L2・partial・fail・inconclusive・claim以外)は**黙って対象外**。
    それらは壊れているのではなく、はじめから trust を上げる資格が無い。
    """
    event_map = {
        str(event.get("event_id")): event
        for event in (events if events is not None else load_events(root))
        if event.get("event_id")
    }
    valid: list[dict] = []
    errors: list[str] = []
    ledger = root / "roundtable" / "brain" / "verifications_v2.jsonl"
    for line_number, row in enumerate(read_jsonl(ledger), 1):
        if (row.get("schema") != VERIFICATION_V2_SCHEMA
                or row.get("target_type") != "claim"
                or row.get("verification_level") not in STRONG_V2_LEVELS
                or row.get("result") != "pass"):
            continue
        mine = _v2_kyouken_no_kizu(root, row, event_map, f"verification_v2:{line_number}")
        if mine:
            errors.extend(mine)
        else:
            valid.append(row)
    return valid, errors


def _v2_kyouken_no_kizu(root: Path, row: dict, event_map: dict, prefix: str) -> list[str]:
    """強検証の1行が有効かを検める(対象の在り処・現行hash・独立・stale旗・証拠の実物SHA)。

    傷の理由の list を返す(空＝有効)。pass の行も反証(fail/partial/inconclusive)の行も同じ関で検める
    (2026-09-27 ai-06fb・王命_20260927-1620: 壊れた反証で正しい pass を消さない)。
    """
    target_id = str(row.get("target_id") or "")
    mine: list[str] = []
    event = event_map.get(target_id)
    if event is None:
        mine.append(f"{prefix}:event_missing:{target_id}")
    else:
        if str(row.get("target_hash") or "") != sha_text(episode_content(event)):
            mine.append(f"{prefix}:target_hash:{target_id}")
        if not v2_verifier_independent(str(event.get("sender") or ""),
                                       str(row.get("author") or ""),
                                       str(row.get("verifier_id") or ""),
                                       mado=str(event.get("mado") or ""),
                                       author_window=str(row.get("author_window") or "")):
            mine.append(f"{prefix}:not_independent:{target_id}")
    if row.get("stale_if_target_changes") is not True:
        mine.append(f"{prefix}:stale_flag:{target_id}")
    refs = row.get("evidence_refs")
    if not isinstance(refs, list) or not refs:
        mine.append(f"{prefix}:evidence_missing:{target_id}")
    else:
        for index, ref in enumerate(refs):
            if not isinstance(ref, dict):
                mine.append(f"{prefix}:evidence_path[{index}]:{target_id}")
                continue
            try:
                evidence = evidence_target(root, str(ref.get("path") or ""))
            except RuntimeError:
                mine.append(f"{prefix}:evidence_path[{index}]:{target_id}")
                continue
            expected = str(ref.get("sha256") or "").lower()
            if len(expected) != 64 or sha_file(evidence) != expected:
                mine.append(f"{prefix}:evidence_hash[{index}]:{target_id}")
    return mine


# ★2026-09-27 ai-06fb（王命_20260927-1620 追補①「反証で降格する」・王「進めていってくれ」）:
#   直す前は L3/L4 pass が1行でも有効なら verified のまま。**後から同じ対象に L3/L4 fail が来ても降りなかった**
#   （くくるの独立 fixture で再現: pass 10:01 → fail 10:02 の後も trust=verified_record）。
#   決まり: 同じ target_id・今の target_hash の**有効な** L3/L4 強検証のうち、最新の1件で決める。
#     ・最新が pass の時だけ上げる。fail/partial/inconclusive なら上げない(既に上がっていれば refresh が追記で降ろす)
#     ・有効か = pass と同じ関(_v2_kyouken_no_kizu)。壊れた反証は最新として扱わない(正しい pass を消させない)
#     ・並び = finished_at。同じ時刻に pass と非pass が並んだら非pass(安全側)。時刻が欠けた/時差の無い行があれば台帳の行順
#     ・v1 の独立検証は今まで通り(verified_event_ids で v1 ∪ ここ)。v2 の反証だけで v1 を消さない
#   世界の型 = 追記だけの台帳は「最新の出来事が勝ち・同時刻は決めきれないので安全側・過去行は消さずに残す」
#     (ctxconnect PR#42 の同意台帳 / AgentCapabilityObservatory PR#43 の食い違う判定の扱い)
V2_HI_PASS = frozenset({"fail", "partial", "inconclusive"})


def _v2_owatta_toki(value) -> datetime | None:
    try:
        when = datetime.fromisoformat(str(value or ""))
    except (TypeError, ValueError):
        return None
    return when if when.tzinfo is not None else None


def v2_saishin_de_kimeru(root: Path, events: list[dict] | None = None) -> tuple[set[str], list[dict]]:
    """有効な L3/L4 強検証の最新の1件で、上げてよい target_id を決める。

    返す物 = (上げてよい target_id の集合, 有効な pass が在るのに反証で上げなかった対象の記録)。
    """
    event_map = {
        str(event.get("event_id")): event
        for event in (events if events is not None else load_events(root))
        if event.get("event_id")
    }
    mure: dict[str, list[tuple[datetime | None, int, str]]] = {}
    ledger = root / "roundtable" / "brain" / "verifications_v2.jsonl"
    for line_number, row in enumerate(read_jsonl(ledger), 1):
        result = str(row.get("result") or "")
        if (row.get("schema") != VERIFICATION_V2_SCHEMA
                or row.get("target_type") != "claim"
                or row.get("verification_level") not in STRONG_V2_LEVELS
                or (result != "pass" and result not in V2_HI_PASS)):
            continue
        if _v2_kyouken_no_kizu(root, row, event_map, f"verification_v2:{line_number}"):
            continue
        mure.setdefault(str(row.get("target_id") or ""), []).append(
            (_v2_owatta_toki(row.get("finished_at")), line_number, result))
    agaru: set[str] = set()
    sagatta: list[dict] = []
    for target_id, gyou in mure.items():
        if any(when is None for when, _, _ in gyou):
            saishin = [max(gyou, key=lambda g: g[1])]
        else:
            toki = max(when for when, _, _ in gyou)
            saishin = [g for g in gyou if g[0] == toki]
        if all(kekka == "pass" for _, _, kekka in saishin):
            agaru.add(target_id)
        elif any(kekka == "pass" for _, _, kekka in gyou):
            sagatta.append({"target_id": target_id,
                            "saishin": [{"line": line, "result": kekka} for _, line, kekka in saishin]})
    return agaru, sagatta


def _v2_kyakka_path(root: Path) -> Path:
    return root / "roundtable" / "brain" / "verifications_v2_kyakka.jsonl"


def v2_kyakka_wo_wakeru(root: Path, errors: list[str]) -> tuple[list[str], list[str]]:
    """v2強検証の却下のうち、受け止め台帳に載った物だけを warning へ回す。

    返す物 = (error に残す物, warning へ回す物)。
    受け止めの一致 = 台帳の行の (verification_id, target_id, reason) が、却下された v2 行と同じ。
    reason は V2_KYAKKA_RIYUU の中だけ。壊れた受け止め行は無視して warning に出す(黙らない)。
    ★この関数は audit の表示だけを変える。trust の昇格(verified_event_ids)はこの台帳を読まない。
    """
    ledger_rows = read_jsonl(root / "roundtable" / "brain" / "verifications_v2.jsonl")
    kyakka: set[tuple[str, str, str]] = set()
    warnings: list[str] = []
    for number, row in enumerate(read_jsonl(_v2_kyakka_path(root)), 1):
        key = (str(row.get("verification_id") or ""), str(row.get("target_id") or ""),
               str(row.get("reason") or ""))
        if (row.get("schema") != V2_KYAKKA_SCHEMA or not all(key)
                or key[2] not in V2_KYAKKA_RIYUU
                or not str(row.get("recorded_by") or "").strip()
                or not str(row.get("note") or "").strip()):
            warnings.append(f"verification_v2_kyakka_invalid:{number}")
            continue
        kyakka.add(key)
    keep: list[str] = []
    for item in errors:
        match = re.match(r"^verification_v2:(\d+):([a-z_]+):(.*)$", item)
        if match:
            line, reason, target = int(match.group(1)), match.group(2), match.group(3)
            row = ledger_rows[line - 1] if 0 < line <= len(ledger_rows) else {}
            key = (str(row.get("verification_id") or ""), target, reason)
            if reason in V2_KYAKKA_RIYUU and key in kyakka:
                warnings.append(item + ":kyakka_zumi")
                continue
        keep.append(item)
    return keep, warnings


# ★2026-09-28 ネオ ai-ecf8 / 札 negai-ko-41a966f59ea3d127
#   （王命_20260927-2247「共有脳audit最終修復：後続の同一SHA正規置換だけevidence_pathをwarning化」・王「進めていってくれ」）
#   実物: verifications_v2.jsonl 96行目は証拠の置き場だけが許可範囲の外(roundtable/shigoto/runtime_output/…)。
#     97行目が同じ bytes を reports/ へ置き直し、それ以外の中身は同じまま追記し直した(追記だけの台帳なので96は消せない)。
#     trust は97で既に正しく上がっているのに、audit だけが96を永久に error にしていた。
#   決まり: 古い行の傷が **evidence_path だけ** で、後の行に「完全な置換」が在る時だけ warning へ回す。
#     完全な置換 = 後の行が有効な L3/L4 pass(_v2_kyouken_no_kizu が空) ／ V2_OKIKAE_ONAJI_RAN が全部同じ
#       (無い欄は同じ欠損・型も区別する) ／ 証拠は path だけ無視して (sha256, source_kind) の列が同じ。
#   ★audit の表示だけを変える。trust の昇格(v2_saishin_de_kimeru / verified_event_ids)は読まない・古い行は有効化しない。
#   ★V2_KYAKKA_RIYUU(手で受け止める理由)は広げない。evidence_hash / target_hash / not_independent 等は必ず error のまま。
V2_OKIKAE_ONAJI_RAN = (
    "target_type", "target_id", "target_hash", "author", "verifier_id", "verification_level",
    "result", "risk_tier", "stale_if_target_changes",
    "verifier_process", "author_window", "verifier_model_family", "author_model_family",
    "verifier_runtime", "test_refs", "observed_outputs", "deviations", "limitations", "confidence",
)
_V2_KETSU = object()   # 欄が無い印(None や "" とは別の物として比べる)


def _v2_ran_no_kao(row: dict, key: str) -> str:
    """欄の値を型ごと比べられる形にする(True と 1、1.0 と 1 を同じにしない)。"""
    value = row.get(key, _V2_KETSU)
    if value is _V2_KETSU:
        return "\x00ketsu"
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def _v2_shouko_no_nakami(row: dict) -> list[tuple[str, str]] | None:
    """証拠の中身の列 = (sha256, source_kind)。path は見ない。形が壊れていれば None(置換と認めない)。"""
    refs = row.get("evidence_refs")
    if not isinstance(refs, list) or not refs:
        return None
    out: list[tuple[str, str]] = []
    for ref in refs:
        if not isinstance(ref, dict):
            return None
        sha = str(ref.get("sha256") or "").lower()
        if len(sha) != 64:
            return None
        out.append((sha, _v2_ran_no_kao(ref, "source_kind")))
    return out


def v2_okikae_wo_wakeru(root: Path, errors: list[str],
                        events: list[dict] | None = None) -> tuple[list[str], list[str]]:
    """古い v2 強検証行の evidence_path の傷のうち、後の行が完全に置き換えた物だけを warning へ回す。

    返す物 = (error に残す物, warning へ回す物)。warning の形 =
      verification_v2:<古い行>:evidence_path[<i>]:<target_id>:replaced_by_valid_line:<後の行>
    その行の傷が1つでも evidence_path 以外なら、その行の傷は全部 error のまま(混ざった行は黙らせない)。
    """
    gyou = re.compile(r"^verification_v2:(\d+):")
    pattern = re.compile(r"^verification_v2:(\d+):([a-z_]+)(?:\[\d+\])?:(.*)$")
    by_line: dict[int, list[str]] = {}
    for item in errors:
        head = gyou.match(item)
        if not head:
            continue
        match = pattern.match(item)
        # 形の分からない傷は「evidence_path ではない傷」として数える(その行は黙らせない)
        by_line.setdefault(int(head.group(1)), []).append(match.group(2) if match else "?")
    if not by_line:
        return list(errors), []
    ledger_rows = read_jsonl(root / "roundtable" / "brain" / "verifications_v2.jsonl")
    event_map = {
        str(event.get("event_id")): event
        for event in (events if events is not None else load_events(root))
        if event.get("event_id")
    }
    okikae: dict[int, int] = {}
    for line, reasons in by_line.items():
        if any(reason != "evidence_path" for reason in reasons):
            continue
        if not 0 < line <= len(ledger_rows):
            continue
        old = ledger_rows[line - 1]
        old_kao = [_v2_ran_no_kao(old, key) for key in V2_OKIKAE_ONAJI_RAN]
        old_shouko = _v2_shouko_no_nakami(old)
        if old_shouko is None:
            continue
        for later in range(line + 1, len(ledger_rows) + 1):
            new = ledger_rows[later - 1]
            if (new.get("schema") != VERIFICATION_V2_SCHEMA
                    or new.get("target_type") != "claim"
                    or new.get("verification_level") not in STRONG_V2_LEVELS
                    or new.get("result") != "pass"):
                continue
            if [_v2_ran_no_kao(new, key) for key in V2_OKIKAE_ONAJI_RAN] != old_kao:
                continue
            if _v2_shouko_no_nakami(new) != old_shouko:
                continue
            if _v2_kyouken_no_kizu(root, new, event_map, f"verification_v2:{later}"):
                continue
            okikae[line] = later
            break
    keep: list[str] = []
    warnings: list[str] = []
    for item in errors:
        head = gyou.match(item)
        if head and int(head.group(1)) in okikae:
            warnings.append(f"{item}:replaced_by_valid_line:{okikae[int(head.group(1))]}")
        else:
            keep.append(item)
    return keep, warnings


def v2_kyakka_wo_kiroku(root: Path, verification_id: str, target_id: str, reason: str,
                        note: str, recorded_by: str) -> dict:
    """却下を見た者が、理由を添えて受け止め台帳へ1行足す(追記だけ)。

    今この瞬間に本当にその理由で却下されている行でなければ断る(先回りの受け止めで黙らせない)。
    """
    if reason not in V2_KYAKKA_RIYUU:
        raise RuntimeError(f"reason must be one of {sorted(V2_KYAKKA_RIYUU)}")
    if not str(note or "").strip() or not str(recorded_by or "").strip():
        raise RuntimeError("note and recorded_by are required")
    _, errors = inspect_strong_verifications_v2(root)
    ledger_rows = read_jsonl(root / "roundtable" / "brain" / "verifications_v2.jsonl")
    lines = []
    for item in errors:
        match = re.match(r"^verification_v2:(\d+):([a-z_]+):(.*)$", item)
        if not match or match.group(2) != reason or match.group(3) != target_id:
            continue
        line = int(match.group(1))
        if 0 < line <= len(ledger_rows) and str(
                ledger_rows[line - 1].get("verification_id") or "") == verification_id:
            lines.append(line)
    if not lines:
        raise RuntimeError("that v2 row is not currently rejected for that reason")
    if any((str(row.get("verification_id") or ""), str(row.get("target_id") or ""),
            str(row.get("reason") or "")) == (verification_id, target_id, reason)
           for row in read_jsonl(_v2_kyakka_path(root))):
        raise RuntimeError("kyakka already recorded")
    row = {
        "schema": V2_KYAKKA_SCHEMA,
        "verification_id": verification_id,
        "target_id": target_id,
        "reason": reason,
        "lines": lines,
        "note": note,
        "recorded_by": recorded_by,
        "recorded_at": datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds"),
    }
    append_jsonl(_v2_kyakka_path(root), [row])
    return {"status": "recorded", **row}


def verification_v2_counts(root: Path, strong_rows: list[dict] | None = None,
                           events: list[dict] | None = None) -> dict[str, int]:
    """v2台帳の表示用の数。**trust の昇格とは別物**（意味の正本は scripts/suuji_no_imi.py）。

    records は生の行数(重複行・partial・L2 を含む)。targets は target_id で畳んだ種類数。
    """
    rows = [row for row in read_jsonl(root / "roundtable" / "brain" / "verifications_v2.jsonl")
            if not row.get("_invalid")]
    if strong_rows is None:
        # 2026-09-27 ai-06fb: 反証で降りた対象を数えない(verified_episodes との差が v1 の重複だけになるように)
        agaru, _ = v2_saishin_de_kimeru(root, events)
        strong_rows = [{"target_id": target} for target in agaru]
    return {
        "verification_v2_records": len(rows),
        "verification_v2_unique_targets": len(
            {str(row.get("target_id")) for row in rows if row.get("target_id")}),
        "verification_v2_pass_targets": len(
            {str(row.get("target_id")) for row in rows
             if row.get("target_id") and row.get("result") == "pass"}),
        "verification_v2_strong_verified_targets": len(
            {str(row.get("target_id")) for row in strong_rows if row.get("target_id")}),
    }


def verified_event_ids(root: Path, events: list[dict] | None = None) -> set[str]:
    """Return only events backed by the independent verification ledger.

    ``chimu_record_memory/verified`` is a derived projection.  In that lane,
    "verified" can mean only that a signed wake reached a terminal state or
    that a report file still matches its indexed hash.  Neither proves the
    event's factual content, so using those filenames as trust authority
    inflated self-reports to ``verified_record``.

    2026-09-27 ai-87bb: v1 の有効行 ∪ v2 の強検証(inspect_strong_verifications_v2 を通った行)。
    event_id / target_id の集合なので、同じ対象に何行あっても1件(重複で水増ししない)。
    """
    if events is None:
        events = load_events(root)
    valid, _ = inspect_verifications(root, events)
    ids = {str(row.get("event_id") or "") for row in valid if row.get("event_id")}
    # 2026-09-27 ai-06fb: v2 は「有効な強検証の最新の1件が pass」の対象だけ（後の反証で降りる）
    agaru, _ = v2_saishin_de_kimeru(root, events)
    ids |= {target for target in agaru if target}
    return ids


def _iso_datetime(value: str) -> datetime | None:
    try:
        return datetime.fromisoformat(str(value or ""))
    except (TypeError, ValueError):
        return None


def jissai_no_tsuzuri(root: Path, rel: str) -> str:
    """rel と綴り(Unicode の NFC/NFD)だけが違う実在パスがあれば、その実際の綴りを返す。無ければ rel のまま。

    macOS 由来の濁点分解(NFD)名のファイルが木に在る(索引 512100 件中 59 件・2026-10-31)。
    人や AI が打ち直す名前は NFC で、Linux の厳密な綴り比較では「無い」扱いになり、訂正登録が
    path_missing で拒まれ、MCP fetch が「読めない」と断っていた(頭脳バグ探し data-1)。
    返すのは綴りだけ。木の外か・読んでよい拡張子かといった安全確認は、呼び手が返った綴りにかけ直す。
    綴り違いが2つ以上に当たる(曖昧な)時は選ばず rel を返す。
    """
    if not rel or rel.isascii() or os.path.isabs(rel):
        return rel
    if os.path.lexists(root / rel):
        return rel
    for form in ("NFD", "NFC"):
        whole = unicodedata.normalize(form, rel)
        if whole != rel and os.path.lexists(root / whole):
            return whole
    # 階層ごとに「NFC にそろえた名前」で突き合わせる(NFC の階層に NFD のファイル、のような混在も拾う)。
    here, parts = Path(root), []
    for part in Path(rel).parts:
        if part in (".", ".."):
            return rel
        if not os.path.lexists(here / part):
            want = unicodedata.normalize("NFC", part)
            try:
                hits = [name for name in os.listdir(here)
                        if unicodedata.normalize("NFC", name) == want]
            except OSError:
                return rel
            if len(hits) != 1:
                return rel
            part = hits[0]
        parts.append(part)
        here = here / part
    return "/".join(parts)


def inspect_corrections(root: Path, events: list[dict] | None = None,
                        catalog: dict[str, dict] | None = None,
                        rows_override: list[dict] | None = None) -> tuple[list[dict], list[str]]:
    """訂正・置換台帳を監査する。

    台帳は履歴を削除しない。correcting_event_id が新しい判断の本文を持ち、targets が
    どの旧event/手順書を、どの期間だけ置換するかを明示する。
    """
    event_rows = events if events is not None else load_events(root)
    event_map = {str(row.get("event_id") or ""): row for row in event_rows}
    event_ids = event_map.keys()
    if catalog is None:
        catalog, _ = report_index(root)
    rows = rows_override if rows_override is not None else read_jsonl(
        root / "roundtable" / "brain" / "corrections.jsonl")
    valid, errors, seen = [], [], set()
    event_edges: dict[str, set[str]] = {}
    for line_number, row in enumerate(rows, 1):
        prefix = f"correction:{line_number}"
        correction_id = str(row.get("correction_id") or "")
        correcting_event_id = str(row.get("correcting_event_id") or "")
        targets = row.get("targets") or []
        valid_from = _iso_datetime(str(row.get("valid_from") or ""))
        valid_until_raw = str(row.get("valid_until") or "")
        valid_until = _iso_datetime(valid_until_raw) if valid_until_raw else None
        match_terms = row.get("match_terms") or []
        if row.get("schema") != CORRECTION_SCHEMA:
            errors.append(f"{prefix}:schema")
        if not correction_id or correction_id in seen:
            errors.append(f"{prefix}:duplicate_or_missing_id")
        seen.add(correction_id)
        if correcting_event_id not in event_ids:
            errors.append(f"{prefix}:correcting_event_missing")
        scope = str(row.get("scope") or "").strip()
        if not scope:
            errors.append(f"{prefix}:scope_missing")
        elif len(scope) > 500:
            errors.append(f"{prefix}:scope_too_long")
        recorded_by = str(row.get("recorded_by") or "").strip()
        if not recorded_by:
            errors.append(f"{prefix}:recorded_by_missing")
        elif len(recorded_by) > 80:
            errors.append(f"{prefix}:recorded_by_too_long")
        reason = str(row.get("reason") or "").strip()
        if not reason:
            errors.append(f"{prefix}:reason_missing")
        elif len(reason) > 1000:
            errors.append(f"{prefix}:reason_too_long")
        if valid_from is None:
            errors.append(f"{prefix}:valid_from")
        if valid_until_raw and valid_until is None:
            errors.append(f"{prefix}:valid_until")
        if valid_from and valid_until:
            start_cmp, end_cmp = valid_from, valid_until
            if start_cmp.tzinfo is None or end_cmp.tzinfo is None:
                start_cmp = start_cmp.replace(tzinfo=None)
                end_cmp = end_cmp.replace(tzinfo=None)
            if end_cmp <= start_cmp:
                errors.append(f"{prefix}:invalid_window")
        if (not isinstance(match_terms, list) or len(match_terms) > 20 or any(
                not isinstance(term, str) or not term.strip() or len(term.strip()) > 80
                for term in match_terms)):
            errors.append(f"{prefix}:match_terms")
            match_terms = []
        elif len({normalize(term) for term in match_terms}) != len(match_terms):
            errors.append(f"{prefix}:duplicate_match_term")
        if not isinstance(targets, list) or not targets:
            errors.append(f"{prefix}:targets_missing")
            targets = []
        elif len(targets) > 50:
            errors.append(f"{prefix}:targets_too_many")
            targets = []
        normalized_targets = []
        target_seen = set()
        for target_index, target in enumerate(targets):
            target_prefix = f"{prefix}:target:{target_index}"
            if not isinstance(target, dict):
                errors.append(f"{target_prefix}:shape")
                continue
            kind = str(target.get("kind") or "")
            value = str(target.get("value") or "")
            if kind == "path":
                # NFC で打たれた名前も、実在(=索引)の綴りへ寄せる。以降の照合・重複判定・検査後の対象が同じ綴りで揃う。
                value = jissai_no_tsuzuri(root, value)
            target_key = (kind, value)
            if target_key in target_seen:
                errors.append(f"{target_prefix}:duplicate")
            target_seen.add(target_key)
            if kind == "event":
                if value not in event_ids:
                    errors.append(f"{target_prefix}:event_missing")
                elif value == correcting_event_id:
                    errors.append(f"{target_prefix}:self_reference")
                elif (str(event_map[value].get("type") or "") not in LOAD_TYPES
                      and str(event_map[value].get("proof") or "")
                      not in {"実測済", "王合格"}):
                    errors.append(f"{target_prefix}:event_not_searchable")
                else:
                    event_edges.setdefault(value, set()).add(correcting_event_id)
            elif kind == "path":
                try:
                    target_path = (root / value).resolve()
                    target_path.relative_to(root.resolve())
                    inside_root = True
                except (OSError, ValueError):
                    inside_root = False
                if not inside_root:
                    errors.append(f"{target_prefix}:path_outside_root")
                elif value not in catalog or not target_path.is_file():
                    errors.append(f"{target_prefix}:path_missing")
                elif (not procedural_target(value, catalog[value])
                      and not any(str(event.get("report_path") or "") == value
                                  and (str(event.get("type") or "") in LOAD_TYPES
                                       or str(event.get("proof") or "")
                                       in {"実測済", "王合格"})
                                  for event in event_rows)):
                    errors.append(f"{target_prefix}:path_not_searchable")
            else:
                errors.append(f"{target_prefix}:kind")
            normalized_targets.append({"kind": kind, "value": value})
        if not any(message.startswith(prefix + ":") for message in errors):
            normalized = dict(row)
            normalized["targets"] = normalized_targets
            normalized["match_terms"] = [str(term).strip() for term in match_terms]
            valid.append(normalized)

    # event A→B→A の訂正循環を拒む。履歴が循環すると「現在」が決められない。
    def reaches(start: str, goal: str, visiting: set[str]) -> bool:
        if start == goal:
            return True
        if start in visiting:
            return False
        visiting.add(start)
        return any(reaches(nxt, goal, visiting) for nxt in event_edges.get(start, set()))

    for source, destinations in event_edges.items():
        if any(reaches(destination, source, set()) for destination in destinations):
            errors.append(f"correction:cycle:{source}")
    if any(message.startswith("correction:cycle:") for message in errors):
        valid = []
    return valid, errors


def record_correction(root: Path, correcting_event_id: str, targets: list[dict],
                      scope: str, reason: str, recorded_by: str,
                      valid_from: str = "", valid_until: str = "",
                      match_terms: list[str] | None = None) -> dict:
    """検証した訂正関係を追記し、共有脳を再生成する。"""
    events = load_events(root)
    event_map = {str(row.get("event_id") or ""): row for row in events}
    correcting = event_map.get(correcting_event_id)
    if correcting is None:
        raise RuntimeError("correcting event not found")
    # 2026-09-09 ネオ: 自己訂正(訂正する側と、対象すべてが同じ発信者)は proof を問わない。
    #   「私の前の発言を取り下げる」は**本人の意思表示**で、外から実測する種類の主張ではない。
    #   実害: この1条件で 2026-09-08 のClaudeの撤回2件(声差し替えの停止範囲)が台帳に入らず、
    #   訂正台帳が 2026-08-28 から12日間止まり、古い停止命令が検索1〜3位に印なしで残った
    #   (くくるの指摘 2026-09-08／実測 2026-09-09)。
    #   判定は**呼び手の申告を信じず、ここで発信者を照合する**(なりすましの壁)。
    #   発信者が空の物は「同じ人」と見なさない。他人の記録を書き換える訂正は今までどおり厳しい。
    _c_sender = str(correcting.get("sender") or "")
    _t_senders = {str((event_map.get(str(target.get("value") or "")) or {}).get("sender") or "")
                  for target in (targets or [])
                  if str(target.get("kind") or "") == "event"}
    #   ★2026-09-09 ネオ(ai-b1)が検める側として穴を実証したので塞いだ ――
    #   _t_senders は kind=="event" の target しか見ない。kind=path は「誰が書いた報告書か」を
    #   照合できない(catalog は発信者を持たない)。そのため
    #     targets = [自分のevent, **他人のpath**]
    #   にすると jiko_teisei が立ち、**proof 無しで他人の記録に⛔を打てた**
    #   (本番データで実証: proof='未検証' の Code の投稿 + Claude の報告書 → 台帳の検査もエラー無し)。
    #   → **照合できない種類の target が1つでも混ざったら、自己訂正として扱わない。**
    #   自分の報告書を自分で訂正したい時は proof を付ければ今までどおり通る(道は塞がない)。
    _t_kinds = {str(target.get("kind") or "") for target in (targets or [])}
    jiko_teisei = bool(_c_sender and _t_senders and _t_senders == {_c_sender}
                       and _t_kinds == {"event"})
    if str(correcting.get("proof") or "") not in {"実測済", "王合格"} and not jiko_teisei:
        raise RuntimeError("correcting event must be 実測済 or 王合格"
                           "（自己訂正なら proof は問わないが、発信者が一致しなかった）")
    valid_from = valid_from or str(correcting.get("timestamp") or "")
    identity = {
        "correcting_event_id": correcting_event_id,
        "targets": targets,
        "scope": str(scope or "").strip(),
        "valid_from": valid_from,
        "valid_until": valid_until,
        "match_terms": [str(term).strip() for term in (match_terms or []) if str(term).strip()],
    }
    correction_id = sha_text(json.dumps(
        identity, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ))[:24]
    existing = read_jsonl(root / "roundtable" / "brain" / "corrections.jsonl")
    if any(str(row.get("correction_id") or "") == correction_id for row in existing):
        raise RuntimeError("correction already recorded")
    row = {
        "schema": CORRECTION_SCHEMA,
        "correction_id": correction_id,
        **identity,
        "reason": redact(str(reason or ""))[:1000],
        "recorded_by": str(recorded_by or "").strip(),
        "recorded_at": now_iso(),
        # なぜ通したかを台帳に残す(後から「なぜ proof 無しで入ったのか」を辿れる形)
        "jiko_teisei": jiko_teisei,
        "correcting_proof": str(correcting.get("proof") or ""),
        "recorded_by_user": os.environ.get("USER", ""),
    }
    catalog, _ = report_index(root)
    staged = existing + [row]
    path = root / "roundtable" / "brain" / "corrections.jsonl"
    _, errors = inspect_corrections(root, events, catalog, staged)
    if errors:
        raise RuntimeError("invalid correction: " + ",".join(errors))
    append_jsonl(path, [row])
    refresh_result = refresh(root)
    return {"correction": row, "refresh": refresh_result}


# ── 同族(kin)= 同じ主張を書いた、名指しされていない投稿 ─────────────────
#
# なぜ要るか(2026-09-08 ネオ・実測):
#   訂正は「この投稿」を名指しで打つ。だが同じ誤りが**別の投稿**にも書いてある事がある。
#   実例: 2026-08-02 11:33 の『受入基準5項目すべて実測で満たした』を 12:07 に訂正したのに、
#   同じ主張を書いた 11:04 の投稿は名指しに入らず、検索で**1位**に印なしで残っていた
#   (問い「生成肌補正v0は受入基準を満たしたのか」で再現)。「1 event = 1 主張」では解けない。
#
# 世界の答え(書く前に読んだ):
#   CUPMem(STALE, arXiv:2605.06527)は同じ穴を Type II「波及する矛盾」と呼び、直接触れた枠の
#   外へ探索を広げて 0.0%→78.0% にした。ただし論文の退役判定は LLM 審判である。
#   そして論文自身が役を分けている ――「広げるのは探索空間、退役は審判が決める」。
#   王国の実測では判定役は同じ記録で19%ぶれる(2026-09-08 第二の目)。だから
#   **広げるのは機械・退役は名指しと審議だけ**に分ける。機械に⛔は打たせない。
#
# 指紋の作り方(閾値を置かない):
#   1. 語は KATAMARI(王国の語彙器)の語だけ。2gram の穴埋めは指紋に使わない。
#   2. 訂正する側と訂正される側の**両方**に出る語を取る。
#   3. その語が出る記録が当事者(訂正側+名指しされた側)だけの物は落とす
#      ―― 定義上、第三の投稿に当たり得ないので指紋にならない。
#   4. 残りを珍しい順に2語。その**両方**を含む記録を同族候補とする。
#
# 2語である根拠(2026-09-08 実測・母数=訂正14件 / episodic 1686行):
#   1語 … 候補15件(真1・偽14)  2語 … 候補4件(真1・偽3)  3語 … 候補0件(真の1件も落ちた)
#   3語で落ちたのは、3番目に珍しい語『画素』を同族の投稿が持っていなかったため。
#   **候補4件は全文をこの目で読んだ。**共有語の数では真偽が分かれない
#   (真=4/11語・偽=6/8, 4/8, 12/29語)ので、数を関所にしない。
#   真の同族は母数14件の訂正の中で**1件しか無い**。n=1で決めた語数である事を隠さない。
#
# 出す印:
#   審議前 … ⚠️(順位は動かさない。読む側が「確かめろ」と分かるだけ)
#   認めた … 名指しと同じ訂正窓を付ける ⇒ 既存の確かな道で ⛔ になる
#   退けた … 何も付けない(以後は黙る)
# 審議の台帳 = roundtable/brain/doukei.jsonl(追記のみ・corrections.jsonl は書き換えない)
DOUKEI_SCHEMA = "kingdom.roundtable.shared_brain.kin.v1"
SHIMON_GO = 2
DOUKEI_HANDAN = ("認める", "退ける")


def shimon_go(text: str) -> set:
    """指紋に使う語。query_terms が足す2gramの穴埋めは指紋には雑音なので使わない
    (2026-09-08 実測: 『9見』『cし』のような2gramが指紋の先頭に立った)。"""
    return {m.group() for m in KATAMARI.finditer(normalize(text or "")) if len(m.group()) >= 2}


def _doukei_hondai(row: dict) -> str:
    return "%s %s" % (row.get("content") or row.get("text") or "", row.get("topic") or "")


def doukei_shimon(source_terms: set, target_terms: list, posting: dict,
                  toujisha: set, kazu: int = 0) -> tuple:
    """(指紋, 使える語ぜんぶ)。当事者の中でしか出ない語は第三の投稿に当たり得ないので落とす。

    kazu の既定は**呼ばれた時**に SHIMON_GO を読む。引数の既定値に定数を書くと def の時に
    焼き付き、後から定数を変えても効かない ―― 変えられない摘みは嘘なので、そうしない
    (2026-09-08 受入試験『骨1』が実際にこの穴を鳴らした)。
    """
    kazu = kazu or SHIMON_GO
    shared = set(source_terms)
    for terms in target_terms:
        shared &= terms
    usable = [word for word in shared if not posting.get(word, frozenset()) <= toujisha]
    usable.sort(key=lambda word: (len(posting.get(word, ())), word))
    return usable[:kazu], usable


def correction_window(correction: dict) -> dict:
    """訂正1件の適用窓。apply_corrections と審議の口で同じ物を使う(定義を割らない)。"""
    return {
        "correction_id": str(correction.get("correction_id") or ""),
        "correcting_event_id": str(correction.get("correcting_event_id") or ""),
        "valid_from": str(correction.get("valid_from") or ""),
        "valid_until": str(correction.get("valid_until") or ""),
        "recorded_at": str(correction.get("recorded_at") or ""),
        "scope": str(correction.get("scope") or ""),
        "match_terms": list(correction.get("match_terms") or []),
    }


def teisei_no_kusari(corrections: list[dict]) -> dict[str, str]:
    """訂正を出した記録が、さらに別の訂正で打ち消されている時の**鎖の終端**を返す。

    なぜ要るか(2026-09-12 ネオ ai-e8ea・隔離試験で実証):
      A(100円) ←B(200円)― ←C(300円)― と連なると、A の印は「訂正=B」を指したままで、
      その B 自身が⛔になっている。**印だけを辿った読み手は、すでに退けられた200円に着く。**
      鎖そのものの状態(A も B も replaced・C が current)は正しかったので、
      直すのは印の指し先だけでよい。

    戻り値 = {打ち消された記録の event_id: 鎖をいちばん後ろまで辿った event_id}
    循環は inspect_corrections が既に拒むが、ここでも見た物を覚えて止める。
    """
    tsugi: dict[str, str] = {}
    for correction in sorted(corrections, key=lambda row: (
            str(row.get("valid_from") or ""), str(row.get("correction_id") or ""))):
        correcting = str(correction.get("correcting_event_id") or "")
        for target in correction.get("targets") or []:
            if str(target.get("kind") or "") != "event":
                continue
            value = str(target.get("value") or "")
            if value and correcting and value != correcting:
                tsugi[value] = correcting          # value を打ち消したのは correcting
    owari: dict[str, str] = {}
    for moto in tsugi:
        saki, mita = tsugi[moto], {moto}
        while saki in tsugi and saki not in mita:
            mita.add(saki)
            saki = tsugi[saki]
        owari[moto] = saki
    return owari


def read_doukei(root: Path) -> dict:
    """同族の審議台帳。同じ組は後の行が勝つ(考えを変えられる)。"""
    saitei = {}
    for row in read_jsonl(root / "roundtable" / "brain" / "doukei.jsonl"):
        key = (str(row.get("correction_id") or ""), str(row.get("event_id") or ""))
        if key[0] and key[1]:
            saitei[key] = row
    return saitei


def doukei_wo_tsukeru(root: Path, memory: dict[str, list[dict]], atehama: list,
                      saitei: dict | None = None,
                      shimon_out: dict | None = None) -> list[dict]:
    """名指しされていない同族へ印を付ける。戻り値は候補一覧(審議の口が使う)。"""
    rows = [row for row in memory.get("episodic", []) if row.get("event_id")]
    if not atehama or not rows:
        return []
    posting: dict[str, set] = {}
    terms: dict[str, set] = {}
    for row in rows:
        event_id = str(row.get("event_id"))
        word_set = shimon_go(_doukei_hondai(row))
        terms[event_id] = word_set
        for word in word_set:
            posting.setdefault(word, set()).add(event_id)
    by_id = {str(row.get("event_id")): row for row in rows}
    saitei = read_doukei(root) if saitei is None else saitei
    kouho: list[dict] = []
    for correction, window, target_ids in atehama:
        correcting = str(correction.get("correcting_event_id") or "")
        targets = [event_id for event_id in dict.fromkeys(target_ids) if event_id in terms]
        if correcting not in terms or not targets:
            continue
        toujisha = {correcting} | set(targets)
        correction_id = str(correction.get("correction_id") or "")
        # 主張の鍵は**訂正そのもの**から作る(指紋から作らない)。
        # ⚠️2026-09-08 実測: 指紋は王国の語の珍しさで決まるので、**新しい投稿が増えるだけで動く**
        #   (この日、私自身の報告2本で「生成肌補正」が df 6→8 になり、df=7 の「画素」に抜かれた)。
        #   鍵が動くと束が割れる。訂正1件=主張1つとして鍵を固定する。
        claim_key = sha_text("claim:" + correction_id)[:16]
        valid_from = str(window.get("valid_from") or "")
        for event_id in toujisha:
            if event_id in by_id:
                by_id[event_id]["claim_key"] = claim_key
        # ★①先に「人が判じた物」を当てる。**機械の見立てが変わっても、人の判断は消えない。**
        #   ⚠️同じ日に踏んだ穴: 判断は台帳に在るのに、指紋が候補を出さなくなった瞬間に
        #   「認める」が黙って効かなくなり、⛔が消えて誤った投稿が検索1位へ戻った。
        #   台帳に在る判断は、指紋を通さずに必ず当てる。
        sumi: set = set()
        for (cid, eid), kime in sorted(saitei.items()):
            if cid != correction_id or eid in toujisha:
                continue
            row = by_id.get(eid)
            if row is None:
                continue
            handan = str(kime.get("handan") or "")
            if handan not in DOUKEI_HANDAN:
                continue
            sumi.add(eid)
            kouho.append({
                "correction_id": correction_id, "correcting_event_id": correcting,
                "event_id": eid, "handan": handan, "riyuu": str(kime.get("riyuu") or ""),
                "claim_key": claim_key, "shimon": list(kime.get("shimon") or []),
                "moto": "台帳(人が判じた)",
                "timestamp": str(row.get("timestamp") or ""), "sender": str(row.get("sender") or ""),
                "targets": list(targets),
            })
            if handan == "認める":
                row["claim_key"] = claim_key
                row.setdefault("supersession_windows", []).append(window)
        # ②機械で新しい候補を探す(まだ誰も判じていない物だけ)
        shimon, usable = doukei_shimon(
            terms[correcting], [terms[event_id] for event_id in targets], posting, toujisha)
        if shimon_out is not None and shimon:
            shimon_out[correction_id] = list(shimon)
        if len(shimon) < SHIMON_GO:
            continue
        matched = set.intersection(*[posting[word] for word in shimon]) - toujisha - sumi
        for event_id in sorted(matched):
            row = by_id.get(event_id)
            if row is None:
                continue
            if valid_from and str(row.get("timestamp") or "") >= valid_from:
                continue      # 訂正より後の投稿は「同じ主張の焼き直し」ではなく別の主張
            kouho.append({
                "correction_id": correction_id,
                "correcting_event_id": correcting,
                "event_id": event_id,
                "handan": "未審議",
                "riyuu": "",
                "claim_key": claim_key,
                "shimon": list(shimon),
                "moto": "指紋(機械が見つけた)",
                "kasanari": len(set(usable) & terms[event_id]),
                "tsukaeru_go": len(usable),
                "timestamp": str(row.get("timestamp") or ""),
                "sender": str(row.get("sender") or ""),
                "targets": list(targets),
            })
            row["claim_key"] = claim_key
            row.setdefault("kin_windows", []).append({**window, "shimon": list(shimon)})
    return kouho


def _doukei_ba(root: Path) -> tuple:
    """審議の場。本番の検索と同じ道(snapshot の episodic)を読む。写しの上で判定する。"""
    snapshot = load_snapshot(root)
    memory = {"episodic": [dict(row) for row in snapshot.get("memory", {}).get("episodic", [])]}
    events = load_events(root)
    catalog, _ = report_index(root)
    corrections, _ = inspect_corrections(root, events, catalog)
    known = {str(row.get("event_id") or "") for row in memory["episodic"]}
    by_report: dict[str, list[str]] = {}
    for row in memory["episodic"]:
        report = str(row.get("report_path") or "")
        if report:
            by_report.setdefault(report, []).append(str(row.get("event_id") or ""))
    atehama = []
    for correction in corrections:
        atari: list[str] = []
        for target in correction.get("targets") or []:
            kind, value = str(target.get("kind") or ""), str(target.get("value") or "")
            if kind == "event" and value in known:
                atari.append(value)
            elif kind == "path":
                atari += by_report.get(value, [])
        atehama.append((correction, correction_window(correction), atari))
    shimon_map: dict = {}
    kouho = doukei_wo_tsukeru(root, memory, atehama, shimon_out=shimon_map)
    return (kouho, {str(row.get("event_id") or ""): row for row in memory["episodic"]},
            shimon_map)


def shimon_ichiran(root: Path) -> dict:
    """訂正ごとの「主張の指紋」。訂正が何について言っているかを2語で指す。
    match_terms が空で scope が定型文の訂正でも、これなら中身で引ける
    (2026-09-08 実測: 定型文をそのまま問いにしていた8件が『訂正が上位に見えない』と鳴っていた)。"""
    return _doukei_ba(root)[2]


def doukei_ichiran(root: Path, mi_shingi_dake: bool = False) -> list[dict]:
    """同族候補の一覧。中身(本文の頭)も付けて、読んで判定できる形で返す。"""
    kouho, by_id, _ = _doukei_ba(root)
    out = []
    for row in kouho:
        if mi_shingi_dake and row.get("handan") != "未審議":
            continue
        moto = by_id.get(str(row.get("correcting_event_id") or "")) or {}
        saki = by_id.get(str(row.get("event_id") or "")) or {}
        out.append({**row,
                    "訂正の言い分": " ".join(str(moto.get("content") or "").split())[:400],
                    "同族の本文": " ".join(str(saki.get("content") or "").split())[:400]})
    return out


def record_doukei(root: Path, correction_id: str, event_id: str, handan: str,
                  riyuu: str, recorded_by: str) -> dict:
    """同族の審議を台帳へ追記する。機械が出していない組は受け付けない(なりすましの壁)。"""
    if handan not in DOUKEI_HANDAN:
        raise RuntimeError("handan must be one of %s" % (DOUKEI_HANDAN,))
    if not str(riyuu or "").strip():
        raise RuntimeError("riyuu is required")
    kouho, _, _ = _doukei_ba(root)
    atari = [r for r in kouho if str(r.get("correction_id")) == correction_id
             and str(r.get("event_id")) == event_id]
    if not atari:
        raise RuntimeError("not a kin candidate: %s / %s" % (correction_id, event_id))
    row = {
        "schema": DOUKEI_SCHEMA,
        "correction_id": correction_id,
        "event_id": event_id,
        "handan": handan,
        # 何を見て判じたかを残す。指紋は語の珍しさで動くので、後から再現できないため。
        "shimon": list(atari[0].get("shimon") or []),
        "claim_key": str(atari[0].get("claim_key") or ""),
        "riyuu": redact(str(riyuu))[:1000],
        "recorded_by": str(recorded_by or "").strip(),
        "recorded_at": now_iso(),
        "recorded_by_user": os.environ.get("USER", ""),
    }
    append_jsonl(root / "roundtable" / "brain" / "doukei.jsonl", [row])
    return {"doukei": row, "refresh": refresh(root)}


def shouko_taba(root: Path, event_id: str) -> dict:
    """同じ主張を束ねて返す(くくるの P0-A「同一事実への複数eventを証拠として束ねる」)。
    原 event まで逆引きできる ―― 訂正した側・名指しされた側・同族を1つの claim_key で結ぶ。"""
    kouho, by_id, _ = _doukei_ba(root)
    row = by_id.get(event_id)
    claim_key = str((row or {}).get("claim_key") or "")
    if not claim_key:
        return {"event_id": event_id, "claim_key": "", "evidence_events": [],
                "note": "この投稿を含む主張の束は無い(訂正が打たれていない)"}
    taba = [k for k in kouho if str(k.get("claim_key")) == claim_key]
    evidence: list[dict] = []
    seen = set()
    def tasu(eid, yakuwari, handan=""):
        if not eid or eid in seen:
            return
        seen.add(eid)
        moto = by_id.get(eid) or {}
        evidence.append({"event_id": eid, "yakuwari": yakuwari, "handan": handan,
                         "timestamp": str(moto.get("timestamp") or ""),
                         "sender": str(moto.get("sender") or ""),
                         "summary": _safe_summary(str(moto.get("summary") or ""), 160)})
    for k in taba:
        tasu(str(k.get("correcting_event_id")), "訂正した側")
        for t in k.get("targets") or []:
            tasu(str(t), "名指しされた側", "認める")
    for k in taba:
        tasu(str(k.get("event_id")), "同族", str(k.get("handan")))
    return {"event_id": event_id, "claim_key": claim_key,
            "shimon": (taba[0].get("shimon") if taba else []),
            "evidence_events": evidence}


def apply_corrections(root: Path, memory: dict[str, list[dict]], events: list[dict],
                      catalog: dict[str, dict]) -> tuple[list[dict], list[str]]:
    """検索用memoryへ訂正鎖を注記する。正本や旧記録は削除しない。"""
    corrections, errors = inspect_corrections(root, events, catalog)
    episodic = {str(row.get("event_id") or ""): row for row in memory.get("episodic", [])}
    path_rows: dict[str, list[dict]] = {}
    sha_rows: dict[str, list[dict]] = {}
    for layer in memory.values():
        for row in layer:
            digest = str(row.get("sha256") or "")
            if len(digest) == 64:
                sha_rows.setdefault(digest, []).append(row)
            for key in ("path", "report_path"):
                value = str(row.get(key) or "")
                if value:
                    path_rows.setdefault(value, []).append(row)
    atehama: list[tuple[dict, dict, list[str]]] = []
    kusari = teisei_no_kusari(corrections)      # 訂正の訂正(鎖)の終端
    _events_by_id = {str(row.get("event_id") or ""): row for row in events}
    for correction in sorted(corrections, key=lambda row: (
            str(row.get("valid_from") or ""), str(row.get("correction_id") or ""))):
        correcting_event_id = str(correction.get("correcting_event_id") or "")
        source = episodic.get(correcting_event_id)
        window = correction_window(correction)
        window["correcting_event_timestamp"] = str(
            (_events_by_id.get(correcting_event_id) or {}).get("timestamp") or "")
        # 訂正側が検索層に載っていない事がある(実測2件/22件)。その時は円卓の正本から引く。
        moto = source if source is not None else _events_by_id.get(correcting_event_id)
        if moto is not None:
            # 訂正側の要点を窓へ入れる。印を読んだだけで「では正しくは何か」が分かるように。
            # 実測: 訂正された記録が結果に出ても訂正の本体が結果に無い事が 18件中4件あり、
            # 「→訂正=<id>」だけでは読む側が正しい答えに辿り着けなかった(2026-09-12)。
            window["youten"] = " ".join(str(
                moto.get("summary") or moto.get("text")
                or moto.get("content") or "").split())[:70]
        # この訂正を出した記録自身が、後で打ち消されていないか(印の指し先を誤らせないため)
        owari = kusari.get(correcting_event_id, "")
        if owari and owari != correcting_event_id:
            window["kusari_owari"] = owari
        if source is not None and source.get("record_id"):
            window["correcting_record_id"] = str(source.get("record_id") or "")
        atari: list[str] = []
        atari_record_ids: list[str] = []
        for target in correction.get("targets") or []:
            kind, value = str(target.get("kind") or ""), str(target.get("value") or "")
            if kind == "event" and value in episodic:
                targets = [episodic[value]]
            elif kind == "path":
                targets = path_rows.get(value, [])
                if not targets:
                    targets = sha_rows.get(str((catalog.get(value) or {}).get("sha256") or ""), [])
            else:
                targets = []
            for target_row in targets:
                if str(target_row.get("event_id") or "") == correcting_event_id:
                    # 訂正を出した記録自身は対象にしない(2026-09-12 本番で踏んだ)。
                    # path/sha 指名は「その道を持つ行」を全部拾うので、訂正側も巻き込む。
                    continue
                target_row.setdefault("supersession_windows", []).append(window)
                if target_row.get("event_id"):
                    atari.append(str(target_row["event_id"]))
                tr_rid = str(target_row.get("record_id") or "")
                if tr_rid and tr_rid not in atari_record_ids:
                    atari_record_ids.append(tr_rid)
        if source is not None:
            had_corr = bool(source.get("correction_id"))
            source["correction_id"] = window["correction_id"]
            if had_corr and source.get("correction_valid_from") and window["valid_from"]:
                source["correction_valid_from"] = min(
                    str(source["correction_valid_from"]), str(window["valid_from"]))
            else:
                source["correction_valid_from"] = window["valid_from"]
            if had_corr:
                prev_vu = str(source.get("correction_valid_until") or "")
                cur_vu = str(window["valid_until"] or "")
                source["correction_valid_until"] = "" if (not prev_vu or not cur_vu) else max(prev_vu, cur_vu)
            else:
                source["correction_valid_until"] = window["valid_until"]
            source["correction_recorded_at"] = window["recorded_at"]
            source["correction_event_timestamp"] = window["correcting_event_timestamp"]
            if had_corr and source.get("correction_scope") and window["scope"] and window["scope"] not in str(source["correction_scope"]):
                source["correction_scope"] = f"{source['correction_scope']} / {window['scope']}"
            else:
                source["correction_scope"] = window["scope"]
            if not had_corr:
                source["correction_match_terms"] = list(window["match_terms"] or [])
            else:
                prev_mt = list(source.get("correction_match_terms") or [])
                cur_mt = list(window["match_terms"] or [])
                if not prev_mt or not cur_mt:
                    source["correction_match_terms"] = []
                else:
                    source["correction_match_terms"] = prev_mt + [t for t in cur_mt if t not in prev_mt]
            merged_targets = list(source.get("corrects") or [])
            for tgt in correction.get("targets") or []:
                if tgt not in merged_targets:
                    merged_targets.append(tgt)
            source["corrects"] = merged_targets
            if atari_record_ids:
                prev_srids = list(source.get("superseded_record_ids") or [])
                for tr_rid in atari_record_ids:
                    if tr_rid not in prev_srids:
                        prev_srids.append(tr_rid)
                source["superseded_record_ids"] = prev_srids
                if not str(source.get("supersedes_record_id") or ""):
                    source["supersedes_record_id"] = prev_srids[0]
        atehama.append((correction, window, atari))
    # 名指しされていない同族へ⚠️を付ける(⛔は打たない)。入口はここ1箇所(教訓9)。
    doukei_wo_tsukeru(root, memory, atehama)
    # ★2026-09-21 ネオ ai-6162: **根拠にした記録が訂正された紙**へ印を付ける。
    moto_ga_teisei_wo_tsukeru(memory, atehama)
    return corrections, errors


def moto_ga_teisei_wo_tsukeru(memory: dict[str, list[dict]], atehama: list) -> int:
    """訂正された記録を**根拠に挙げている** episodic 以外の記録へ印を付ける。

    ★2026-09-21 ネオ ai-6162 が実測して見つけた穴（王命_20260921-0059 P0/Acceptance H）:
      訂正は episodic(検索の写し)と facts には 27/27 届いていたのに、
      **procedural(提案書・報告書・話題のMarkdown)には0件**しか届いていなかった。
      実物: `proposals/Claude提案_20260908-0031_….md` は本文に
      「Claudeの深掘り報告（円卓 AI-頭脳 event 01788794756216402199-8592f682）」と出典を書いている。
      その event は訂正済みなのに、検索でこの提案書が**2位・無印**で返っていた
      （訂正された記録自身は上位に出ないので、読み手は訂正に気づけない）。
      世界の名 = cascade update problem（MEMOREPAIR arXiv:2605.07242・
      「元が訂正・撤回されても、そこから派生した物が古い裏付けのまま見え続ける」）。

    ★**⛔は打たない。**その紙が打ち消されたのではなく、**根拠にした記録が訂正された**だけ。
      世界の作法 = 訂正の影響は二値ではない（選択的な取り消し・再検証であって、全無効ではない。
      SCLib #66 / Sara_pro #453）。どこがどう変わるかの判定は人とAIの仕事。ここは告知だけ。

    ⚠️ episodic は対象にしない（既に supersession_windows と同族の印が在る。二重に付けない）。
    """
    tsuita = 0
    for sou, rows in memory.items():
        if sou == "episodic":
            continue
        for correction, window, target_ids in atehama:
            mato = [e for e in dict.fromkeys(target_ids) if e]
            if not mato:
                continue
            for row in rows:
                honbun = " ".join(str(row.get(k) or "")
                                  for k in ("text", "summary", "digest", "title", "path"))
                ataru = [e for e in mato if e and e in honbun]
                if not ataru:
                    continue
                mochi = row.setdefault("moto_ga_teisei_sareta", [])
                if any(str(x.get("correction_id")) == window.get("correction_id") for x in mochi):
                    continue
                mochi.append({"correction_id": window.get("correction_id"),
                              "correcting_event_id": window.get("correcting_event_id"),
                              "moto_no_event_id": ataru[0],
                              "valid_from": window.get("valid_from"),
                              "recorded_at": window.get("recorded_at"),
                              "correcting_event_timestamp": window.get("correcting_event_timestamp"),
                              "scope": str(window.get("scope") or "")[:120]})
                tsuita += 1
    return tsuita


_INDEX_OBOE: dict = {}


def report_index(root: Path) -> tuple[dict[str, dict], dict]:
    path = root / "reports" / "索引" / "_記録総合索引.json"
    # 2026-10-03 ai-3170(札 6246・候補): 107MB を毎回読み直さない。中身が同じ間(同じ stat)だけ覚える。
    #   返す対応表は浅い写し(呼び手が足し引きしても覚えた物が汚れない)。更新から2秒以内は覚えない。
    try:
        kagi, mtime_ns = _stat_kagi(path)
    except OSError:
        kagi, mtime_ns = None, 0
    hit = _INDEX_OBOE.get(str(path))
    if kagi is not None and hit and hit[0] == kagi:
        return dict(hit[1]), hit[2]
    ima = _jikan_ai3170.time_ns()
    mapping, data = _report_index_yomu(path)
    # ★2026-10-03 ai-6c56(札 c477 反証1回目 A): 読めなかった時(read_json は OSError で {} を返す)を覚えない。
    #   失敗は覚えない(覚えるなら成功よりずっと短く＝世界の negative caching の作法)。
    #   records が list として読めた時だけ覚える。読んだ後の stat が読む前と同じ時だけ。
    yometa = isinstance(data, dict) and isinstance(data.get("records"), list)
    if (kagi is not None and yometa and ima - mtime_ns > _OBOE_YOYUU_NS
            and _onaji_mama(path, kagi)):
        _INDEX_OBOE[str(path)] = (kagi, mapping, data)
        return dict(mapping), data
    return mapping, data


def _report_index_yomu(path: Path) -> tuple[dict[str, dict], dict]:
    data = read_json(path, {})
    rows = data.get("records") if isinstance(data, dict) else []
    mapping = {}
    for row in rows or []:
        if isinstance(row, dict) and row.get("path"):
            mapping[str(row["path"])] = row
    return mapping, data if isinstance(data, dict) else {}


def report_is_verified(root: Path, report_path: str, catalog: dict[str, dict]) -> bool:
    report_path = jissai_no_tsuzuri(root, report_path)   # NFC で打たれても索引の綴りで確かめる
    if not report_path or report_path not in catalog:
        return False
    target = (root / report_path).resolve()
    try:
        target.relative_to(root.resolve())
    except ValueError:
        return False
    expected = str(catalog[report_path].get("sha256") or "")
    return target.is_file() and len(expected) == 64 and sha_file(target) == expected


def teisei_wo_dashita_ids(root: Path, events: list[dict], catalog: dict[str, dict]) -> set[str]:
    """有効な訂正を出した記録の event_id。頭脳に載る門でこれを通す。

    2026-09-12 ネオ ai-e8ea 実測: 訂正22件のうち2件の**訂正側が検索層に居なかった**。
    訂正の入口は「自己訂正なら proof を問わない」で通すのに、頭脳に載る門は
    proof=実測済/王合格 を要求する ―― **門が2つあって、片方だけ通った記録が生まれる**。
    訂正が有効なら、その本体は「今の正しい主張」。引けなければ訂正の意味が半分になる。
    """
    try:
        corrections, _ = inspect_corrections(root, events, catalog)
    except Exception:
        return set()                     # 訂正台帳が読めなくても脳の構築は止めない
    return {str(row.get("correcting_event_id") or "") for row in corrections
            if str(row.get("correcting_event_id") or "")}


def _episode_trust(event_id: str, proof: str, verified_ids: set[str]) -> str:
    if event_id in verified_ids:
        return "verified_record"
    # A report SHA establishes integrity; strong self-reported proof remains measured.
    return "measured_claim" if proof in ("実測済", "王合格") else "candidate"


def _episode_loadable(event: dict, event_id: str, event_type: str, proof: str,
                      teisei_ids: set[str] | None) -> bool:
    assessment = event.get("proof_assessment") or {}
    requested = str(event.get("proof_requested") or "")
    return (event_type in LOAD_TYPES or proof in ("実測済", "外部裏取り", "王合格")
            or event_id in (teisei_ids or set())
            or (bool(assessment) and requested in ("実測済", "外部裏取り", "王合格")))


def _episode_can_reuse(previous: dict, event: dict, event_id: str,
                        verified_ids: set[str], attachments: list[dict],
                        teisei_ids: set[str] | None = None) -> bool:
    if (attachments or previous.get("attachments_sha256")
            or ":legacy:" in str(previous.get("record_id") or "")):
        return False
    proof = str(event.get("proof") or "未検証")
    if (previous.get("proof") != proof
            or previous.get("trust") != _episode_trust(event_id, proof, verified_ids)
            or str(previous.get("report_path") or "") != str(event.get("report_path") or "")
            or str(previous.get("source") or "") != str(event.get("_source") or "")):
        return False
    if not _episode_loadable(event, event_id, str(event.get("type") or ""), proof, teisei_ids):
        return True   # No candidate row; the persisted latest row stays current.
    assessment = event.get("proof_assessment")
    if isinstance(assessment, dict):
        claims = assessment.get("claims", [])
        if not isinstance(claims, list) or any(not isinstance(claim, dict) for claim in claims):
            return False   # Preserve the converter's behavior on malformed proof data.
    return previous.get("content_sha256") == sha_text(episode_content(event))



def episode_from_event(root: Path, event: dict, verified_ids: set[str], catalog: dict[str, dict],
                       attachments: list[dict] | None = None,
                       teisei_ids: set[str] | None = None) -> dict | None:
    event_id = str(event.get("event_id") or "")
    if not event_id:
        return None
    event_type = str(event.get("type") or "")
    proof = str(event.get("proof") or "未検証")
    # 2026-08-20: 王命(instruction)・質問・提案を載せるようにした。
    # 実測: この門で1,593件中775件のnoteが落ち、**王命が10件、脳に載っていなかった**。
    # 王命は「実測済」の印が付く種類の投稿ではないので、実測を求める門が王の言葉自身を弾いていた。
    # 2026-08-31 修理(バグ狩りC6): proof=王合格のnoteも載せる。王国唯一の王合格noteが
    # episodicに不在だった(01787638016301677961-95bcf25b・静止画基準)。
    # ★2026-09-21 ネオ ai-6162（王の許し「すすめていいよ」）:
    #   **引用の錠に印を下げられた記録も、candidate として載せる。**
    #
    #   何が起きていたか（実測）: 引用の錠が 2026-09-20T02:54 に生きた。錠は正しい ――
    #   受領書の無い投稿の `proof` を `未確認` にする。ところが**この門は `proof` を見ている**ので、
    #   錠が締まった瞬間にこの門も一緒に締まった。同じ物の持ち主が2人いた。
    #     9/19 = 678件が載った → 9/20 = 275件 → **9/21 = 17件**（載らない率 85.2%）
    #     trust=measured_claim は 636件 → **1件**
    #   二重に確かめた: ①この関数をそのまま呼んで生イベントを食わせた
    #   ②実物の roundtable/brain/episodes.jsonl(4,698行)を日付で数えた。1件も食い違わなかった。
    #   前の数字の焼き付け = reports/torikomi_20260921/mae_*.txt（書き換えない）
    #
    #   ★世界の声（2026-09-21 にディープサーチ。王命_20260921-0059 が引く資料も含む）
    #     ・Agent Zero Memory (arXiv:2608.29606) ―― **引用の錠は「答える時」に働く。**
    #       証拠の道が組めなければ**答えを断る**。取り込みの時に記録を消すとは書いていない。
    #     ・Zep ―― 「問題のある記憶を**削除するのではなく**、隔離する・信頼度を下げる・見逃しを公表する」
    #     ・PraisonAI #5028 / PR #5042 ―― 出所が信用できない内容は**長期への昇格から外すか、
    #       囲いを付けて取り出す**。黙って信用された物として扱わない（＝消すのではない）
    #   王命 P0 の言葉でも「**引用可能性≠真実性を別軸維持**」。
    #   trust の軸(candidate/measured_claim/verified_record)は既に在り、
    #   囲い（⚠中身は未検証）も既に在る。正しい壊れ方は「candidate で載る」であって「載らない」ではない。
    #
    #   ⚠️**広げすぎない。**最初から何も名乗っていない投稿(未検証)は、錠の前も後も載らない ――
    #   ここで戻すのは「**名乗ったのに錠に下げられた物**」だけ。錠が入る前の姿と同じ集合になる。
    if not _episode_loadable(event, event_id, event_type, proof, teisei_ids):
        # 2026-09-12: 有効な訂正を出した記録は proof に関わらず載せる(門が2つあった穴)。
        return None
    report_path = str(event.get("report_path") or "")
    trust = _episode_trust(event_id, proof, verified_ids)
    content = episode_content(event)
    attachment_rows = attachments or []
    attachment_sha = sha_text(json.dumps(
        attachment_rows, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    )) if attachment_rows else ""
    row = {
        "schema": EPISODE_SCHEMA,
        "event_id": event_id,
        "thread_id": str(event.get("thread_id") or ""),
        "timestamp": str(event.get("timestamp") or ""),
        "sender": str(event.get("sender") or ""),
        "to": str(event.get("to") or ""),
        "type": event_type,
        "topic": str(event.get("topic") or ""),
        "proof": proof,
        "report_path": report_path,
        "memory_type": "episodic",
        "trust": trust,
        "content": content,
        "content_sha256": sha_text(content),
        "source": str(event.get("_source") or ""),
        "attachments": attachment_rows,
        "attachments_sha256": attachment_sha,
    }
    if isinstance(event.get("proof_assessment"), dict):
        pa = event["proof_assessment"]
        row["proof_basis"] = {
            "schema": pa.get("schema"), "evaluated_at": pa.get("evaluated_at"),
            "requested": event.get("proof_requested"), "uncovered_text": pa.get("uncovered_text"),
            "claim_ids": [c.get("claim_id") for c in pa.get("claims", [])],
            "truth_verification": "not_established", "detail_event_id": event_id}
    identity_fields = {
        key: row[key] for key in
        ("event_id", "trust", "proof", "report_path", "content_sha256", "source")
    }
    if attachment_sha:
        identity_fields["attachments_sha256"] = attachment_sha
    identity = json.dumps(
        identity_fields,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    row["record_id"] = f"{event_id}:{sha_text(identity)[:16]}"
    return row


def episodes_equivalent(previous: dict, current: dict) -> bool:
    keys = (
        "event_id", "trust", "proof", "report_path", "content_sha256", "source",
        "attachments_sha256",
    )
    return all(str(previous.get(key) or "") == str(current.get(key) or "") for key in keys)


def load_or_append_episodes(root: Path, events: list[dict], catalog: dict[str, dict],
                            *, revision_count: list[int] | None = None) -> list[dict]:
    path = root / "roundtable" / "brain" / "episodes.jsonl"
    existing = read_jsonl(path)
    latest = {}
    existing_record_ids: set[str] = set()
    for line_number, row in enumerate(existing, 1):
        event_id = str(row.get("event_id") or "")
        if not event_id:
            continue
        normalized = row
        if "revision" not in row or "record_id" not in row:
            normalized = dict(row)
            normalized.setdefault("revision", 1)
            normalized.setdefault("record_id", f"{event_id}:legacy:{line_number}")
        latest[event_id] = normalized
        existing_record_ids.add(str(normalized["record_id"]))
    verified_ids = verified_event_ids(root, events)
    teisei_ids = teisei_wo_dashita_ids(root, events, catalog)
    media_by_event = roundtable_media.attachments_by_event(root)
    additions = []
    for event in events:
        event_id = str(event.get("event_id") or "")
        previous = latest.get(event_id)
        attachments = media_by_event.get(event_id, [])
        if previous and _episode_can_reuse(previous, event, event_id, verified_ids, attachments, teisei_ids):
            continue
        episode = episode_from_event(root, event, verified_ids, catalog, attachments, teisei_ids)
        if not episode:
            continue
        if previous and episodes_equivalent(previous, episode):
            # Keep the persisted latest revision.  Rebuilding a logically
            # identical row must not replace its unique transition record_id.
            # Legacy rows are the exception: replace their synthesized
            # ``:legacy:`` identifier in memory without appending a mass
            # migration to the append-only ledger.
            if ":legacy:" in str(previous.get("record_id") or ""):
                episode["revision"] = int(previous.get("revision") or 1)
                episode["supersedes_record_id"] = str(
                    previous.get("supersedes_record_id") or "")
                latest[event_id] = episode
            else:
                latest[event_id] = previous
            continue
        revision = int(previous.get("revision") or 1) + 1 if previous else 1
        supersedes = str(previous.get("record_id") or "") if previous else ""
        if episode["record_id"] in existing_record_ids:
            # Trust may legitimately return to an older state (for example
            # verified -> candidate after a derived proof is rejected).  The
            # state identity then matches a historical record_id.  Reusing it
            # would violate audit uniqueness, while silently skipping would
            # leave the stale stronger revision current.  Give the transition
            # its own deterministic identity and append it once.
            base_record_id = str(episode["record_id"])
            transition = json.dumps({
                "base_record_id": base_record_id,
                "revision": revision,
                "supersedes_record_id": supersedes,
            }, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
            episode["record_id"] = f"{event_id}:{sha_text(transition)[:16]}"
        episode["revision"] = revision
        episode["supersedes_record_id"] = supersedes
        additions.append(episode)
        latest[event_id] = episode
        existing_record_ids.add(str(episode["record_id"]))
    # 訂正台帳(corrections.jsonl)で旧イベントを訂正しているイベントは、
    # episodes.jsonl の過去行を書き換えずに supersedes_record_id / superseded_record_ids
    # を結線した新しい revision 行を1回だけ追記する(冪等)。
    try:
        corr_rows, corr_errs = inspect_corrections(root, events, catalog)
    except Exception:
        corr_rows, corr_errs = [], ["inspect_failed"]
    if corr_rows and not corr_errs:
        by_path_rids: dict[str, list[tuple[str, str]]] = {}
        for ev_id_k, ep_row in latest.items():
            rp = str(ep_row.get("report_path") or "")
            rid = str(ep_row.get("record_id") or "")
            if rp and rid:
                by_path_rids.setdefault(rp, []).append((ev_id_k, rid))
        corr_targets_by_src: dict[str, list[str]] = {}
        for corr in sorted(
            corr_rows,
            key=lambda row: (str(row.get("valid_from") or ""), str(row.get("correction_id") or "")),
        ):
            src_eid = str(corr.get("correcting_event_id") or "")
            if not src_eid or src_eid not in latest:
                continue
            rids = corr_targets_by_src.setdefault(src_eid, [])
            for tgt in corr.get("targets") or []:
                kind = str(tgt.get("kind") or "")
                val = str(tgt.get("value") or "")
                if kind == "event" and val and val != src_eid and val in latest:
                    tr_rid = str(latest[val].get("record_id") or "")
                    if tr_rid and tr_rid not in rids:
                        rids.append(tr_rid)
                elif kind == "path" and val:
                    for ev_id_k, tr_rid in by_path_rids.get(val, []):
                        if ev_id_k != src_eid and tr_rid and tr_rid not in rids:
                            rids.append(tr_rid)
        for src_eid, target_rids in corr_targets_by_src.items():
            if not target_rids:
                continue
            src_prev = latest.get(src_eid)
            if not src_prev:
                continue
            desired_primary = target_rids[0]
            cur_primary = str(src_prev.get("supersedes_record_id") or "")
            cur_all = [str(x) for x in (src_prev.get("superseded_record_ids") or []) if str(x)]
            if cur_primary == desired_primary and cur_all == target_rids:
                continue
            new_ep = dict(src_prev)
            new_rev = int(src_prev.get("revision") or 1) + 1
            base_rid = str(src_prev.get("record_id") or "")
            while True:
                transition = json.dumps({
                    "base_record_id": base_rid,
                    "revision": new_rev,
                    "supersedes_record_id": desired_primary,
                    "superseded_record_ids": target_rids,
                }, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
                new_rid = f"{src_eid}:{sha_text(transition)[:16]}"
                if new_rid not in existing_record_ids:
                    break
                new_rev += 1
            new_ep["record_id"] = new_rid
            new_ep["revision"] = new_rev
            new_ep["supersedes_record_id"] = desired_primary
            new_ep["superseded_record_ids"] = list(target_rids)
            additions.append(new_ep)
            latest[src_eid] = new_ep
            existing_record_ids.add(new_rid)
    append_jsonl(path, additions)
    if revision_count is not None:
        revision_count.append(len(existing) + len(additions))
    return sorted(latest.values(), key=lambda item: (str(item.get("timestamp") or ""), str(item.get("event_id") or "")))


def inspect_resolutions(root: Path) -> tuple[list[dict], list[str], int]:
    """Return one current resolution per question without rewriting history.

    ``resolutions.jsonl`` is append-only.  A correction can therefore add a
    second row for the same question.  Search used to index both rows and a
    later generic ID de-duplication could keep the obsolete answer merely
    because it had a higher lexical score.  Resolve versions before ranking:
    the greatest valid ``resolved_at`` wins, with file order as a deterministic
    tie-breaker.  Invalid timestamps are visible to audit and cannot supersede
    a dated row.
    """
    current: dict[str, tuple[tuple[float, int], dict]] = {}
    errors: list[str] = []
    revisions = 0
    path = root / "roundtable" / "resolutions.jsonl"
    for line_number, row in enumerate(read_jsonl(path), 1):
        if row.get("_invalid"):
            errors.append(f"resolution_invalid_json:{row.get('_line') or line_number}")
            continue
        question_id = str(row.get("question_event_id") or "")
        if not question_id:
            errors.append(f"resolution_missing_question_id:{line_number}")
            continue
        resolved_at = str(row.get("resolved_at") or "")
        when = _iso_datetime(resolved_at)
        if when is None:
            errors.append(f"resolution_invalid_resolved_at:{line_number}:{question_id}")
            # 2026-09-12 くくる監査③: 全行invalidの質問は current に残さない。
            #   従来は rank=-inf の行が初回登録され、後続に正常行が無ければ semantic memory
            #   まで invalid のまま流れていた。errors には既に追加済みなので、ここで打ち切る。
            if question_id in current:
                revisions += 1
            continue
        if when.tzinfo is None:
            when = when.replace(tzinfo=timezone.utc)
        rank = (when.timestamp(), line_number)
        if question_id in current:
            revisions += 1
        if question_id not in current or rank > current[question_id][0]:
            current[question_id] = (rank, row)
    selected = [value[1] for value in current.values()]
    selected.sort(key=lambda row: (
        str(row.get("resolved_at") or ""), str(row.get("question_event_id") or "")))
    return selected, errors, revisions


def semantic_memories(root: Path, event_map: dict[str, dict]) -> list[dict]:
    rows = []
    youten_map = load_youten(root)   # 要点1行はepisodicだけでなく解決済みにも付ける(2026-08-20 ネオ)
    current_resolutions, _, _ = inspect_resolutions(root)
    for resolution in current_resolutions:
        question_id = str(resolution.get("question_event_id") or "")
        if not question_id:
            continue
        answer_id = str(resolution.get("answer_event_id") or "")
        question = event_map.get(question_id, {})
        answer = event_map.get(answer_id, {})
        text = redact(
            "質問: " + str(question.get("content") or "") + "\n回答: " + str(answer.get("content") or "")
        )[:MAX_EVENT_CHARS]
        answer_body = redact(str(answer.get("content") or ""))[:MAX_EVENT_CHARS]
        # 要点は「回答」から取る。答えの無い解決は質問文から(episodicと同じsha計算でサイドカーが効く)
        youten = summary_line(answer_body, sha_text(answer_body), youten_map) if answer_body else ""
        if not youten:
            youten = " ".join(str(question.get("content") or "").split())[:120]
        rows.append({
            "id": question_id,
            "summary": _safe_summary(youten, 160),
            "memory_type": "semantic",
            "trust": "resolved",
            "timestamp": str(resolution.get("resolved_at") or ""),
            "sender": str(answer.get("sender") or resolution.get("answer_sender") or ""),
            "to": str(question.get("to") or resolution.get("question_to") or ""),
            "topic": str(question.get("topic") or ""),
            "text": text,
            "source": "roundtable/resolutions.jsonl",
            "question_event_id": question_id,
            "answer_event_id": answer_id,
            "note": redact(str(resolution.get("note") or ""))[:1000],
        })
    rows.sort(key=lambda item: (item["timestamp"], item["id"]))
    return rows


def media_memories(root: Path) -> list[dict]:
    """登録済み媒体をevent型に関係なく検索できる共有脳レコードへ変換する。

    noteはepisodic層の採用条件から外れるため、添付だけに依存すると元動画が
    台帳には在るのに共有脳検索へ出ない。媒体のSHA整合性と、内容の真偽・合否は
    別物なので trust/proof も分けて表す。
    """
    rows = []
    for media in roundtable_media.load_manifest(root):
        media_id = str(media.get("media_id") or "")
        if not media_id:
            continue
        display_name = str(media.get("display_name") or media_id)
        tags = [str(tag) for tag in (media.get("tags") or []) if str(tag)]
        kind = str(media.get("kind") or "media")
        duration = media.get("duration_seconds")
        dimensions = ""
        if media.get("width") and media.get("height"):
            dimensions = f"{media['width']}x{media['height']}"
        facts = [
            display_name,
            media_id,
            kind,
            str(media.get("mime_type") or ""),
            str(media.get("role") or ""),
            " ".join(tags),
            str(media.get("source_path") or ""),
            str(media.get("storage_relpath") or ""),
            dimensions,
            f"{duration}秒" if duration is not None else "",
        ]
        summary_bits = [display_name, kind]
        if duration is not None:
            summary_bits.append(f"{duration}秒")
        if dimensions:
            summary_bits.append(dimensions)
        rows.append({
            **media,
            "id": media_id,
            "title": display_name,
            "memory_type": "media",
            "trust": "registered_media",
            "proof": "媒体SHA256整合性",
            "timestamp": str(media.get("registered_at") or ""),
            "type": "media",
            "topic": " ".join(tags),
            "to": "全員",
            "source": str(media.get("storage_relpath") or "roundtable/media/manifest.jsonl"),
            "summary": " / ".join(summary_bits),
            "text": " ".join(part for part in facts if part),
            "attachments": [media],
        })
    return rows


COPY_MARKERS = ("backups/", ".bak", "/話題.bak", "旧_")
_PROCEDURAL_ALWAYS = {"00_共通メモリ", "01_正式指示"}
_PROCEDURAL_EXCLUDE_RE = re.compile("|".join(map(re.escape, dict.fromkeys(COPY_MARKERS + GOMI_MARKERS))))
_PROCEDURAL_WORD_RE = re.compile("|".join(map(re.escape, PROCEDURAL_WORDS)))


def prefer_original(left: dict, right: dict) -> dict:
    """同じ中身が二重に索引されている時、円卓側の原本を残す(RAG複製を畳む)。"""
    for row in (left, right):
        if str(row.get("path") or "").startswith("roundtable/"):
            return row
    return left if len(str(left.get("path") or "")) <= len(str(right.get("path") or "")) else right


def _procedural_cache_dir() -> Path | None:
    env = os.environ.get("KINGDOM_PROCEDURAL_CACHE_DIR")
    if env is not None:
        v = env.strip()
        return None if v in ("", "0", "off", "false", "no") else Path(v)
    if (_CODE_DIR.parent.name, _CODE_DIR.name) == ("work", "kukuru_heya"):
        return _CODE_DIR / ".brain-speed-cache-20261121"
    return Path.home() / ".cache" / "kingdom_brain_speed"


_PROCEDURAL_CACHE = ProceduralFiles(_procedural_cache_dir())
_PROCEDURAL_PROVENANCE = ("shussho", "shinrai", "gaibu_no_ato", "jikkou_shinrai")
_PROCEDURAL_VERSION = None


def _procedural_cache_version():
    global _PROCEDURAL_VERSION
    if _PROCEDURAL_VERSION is None:
        try:
            _PROCEDURAL_VERSION = sha_file(Path(__file__)) + sha_file(Path(_shussho_michibiku.__code__.co_filename))
        except OSError:
            return ""
    return _PROCEDURAL_VERSION


def _procedural_file(root, path, target, stamp, head_key):
    if head_key is None:
        head_row, head_stable = record_head(root, path), False
    else:
        head_row, head_stable = _record_head_cached(target, head_key, 900)
    body = record_body(root, path, _observed_fingerprint=stamp)
    body_ok = ("path", str(target.absolute()), stamp) in BODY_CACHE
    origin = _shussho_fuda(path, body if body_ok else record_body(root, path))
    source = (stamp, *head_row, body, tuple(origin[key] for key in _PROCEDURAL_PROVENANCE))
    # 同じmtime・サイズの差し替えで、旧HEAD_CACHEが返した要約は次の起動へ持ち越さない。
    return source, body_ok and head_stable and _onaji_mama(target, stamp)


def procedural_memories(root: Path, catalog: dict[str, dict]) -> list[dict]:
    rows = []
    with _PROCEDURAL_CACHE.use(root, _procedural_cache_version()) as (previous, current):
        for path, record in catalog.items():
            if not procedural_target(path, record):
                continue
            target = root / path
            try:
                stamp, _ = _stat_kagi(target)
            except OSError:
                stamp = None
            head_key = (os.path.realpath(target), stamp[2], stamp[3], 900) if stamp else None
            source = previous.get(path)
            if source is not None and source[0] == stamp:
                current[path] = source
                head_row, _ = _record_head_cached(target, head_key, 900, source[1:3])
            else:
                source, stable = _procedural_file(root, path, target, stamp, head_key)
                if stable:
                    current[path] = source
                head_row = source[1:3]
            head, is_digest = head_row
            _, _, _, body, provenance = source
            title = re.sub(r"（追記のみ[^）]*）", "", str(record.get("title") or Path(path).stem)).strip()
            head_first = head.split("\n", 1)[0]
            rows.append({
                "id": sha_text(path)[:16],
                "memory_type": "procedural",
                "trust": "indexed_record",
                "timestamp": str(record.get("mtime") or ""),
                "title": redact(title)[:300],
                "path": path,
                "proof": str(record.get("proof_hint") or "記載なし"),
                "sha256": str(record.get("sha256") or ""),
                "source": "reports/索引/_記録総合索引.json",
                "digest": is_digest,
                "summary": _safe_summary((head_first or title), 160),
                "text": (title + " " + path + " " + body)[:MAX_BODY_CHARS],
                **dict(zip(_PROCEDURAL_PROVENANCE, provenance)),
            })
    folded: dict[str, dict] = {}
    loose = []
    for row in rows:
        sha = str(row.get("sha256") or "")
        if len(sha) != 64:
            loose.append(row)
            continue
        previous = folded.get(sha)
        folded[sha] = prefer_original(previous, row) if previous else row
    merged = list(folded.values()) + loose
    merged.sort(key=lambda item: (item["timestamp"], item["path"]))
    return merged


def dougu_procedural_memories(root: Path) -> list[dict]:
    """王国の道具(.py)の「何のための道具か」を procedural に載せる。

    2026-09-09 ネオ。作りは既存の workbench_code_procedural_memories に合わせてある
    (道具台帳=作業台に登録した実行できる9本／こちら=王国が書いた道具615本の説明。補完関係)。

    なぜ要るか(実測):
      検索の成績を問いの種類で割ったら、**A型(王の言い方・語がずれる型)だけが弱い**と分かった。
        A型 66問: 1位 37.9% ／ 15問(22.7%)は50位以内にすら入らない
        D型(固有名詞)174問: 1位 60.9% ／ 50位圏外は 3問(1.7%)
      落ちた15問は全部「うちの絵を作るやつ」「うちの音を作るやつ」「あの自動で治すやつ」――
      **王の言い方**。記録側は chimu-v7 / acestep-api / router_v4.py と固有名詞で書かれている。
      そして procedural 層 757件は reports 725 と roundtable 32 だけで、
      **王国の道具の説明は1件も入っていなかった**(記憶 feedback-dougu-ga-aru-noni-tsukawanai)。

    橋になる理由(実測で確かめた):
      「絵」で道具を引くと patch_illust3.py「**チム**が王の手本と同じ細かさで**絵**の指示を
      書けるようにする」が出る ―― **「絵」と「チム」を繋ぐ一文が道具の説明の中に在る**。
      正解の記録の本文には「絵」が一度も出てこない(chimu-v7 だけ)ので、この橋が無いと辿れない。

    世界の答え(書く前に読んだ):
      語彙ギャップは古典的な問題で、橋を架ける確立手法は**統制語彙**(概念ごとに優先語を1つ持ち、
      非優先語からリンクする)。出典 arXiv:1711.06004 / WebDB Terminology Gap。
      リランカーは精度の道具で再現の道具ではない ―― 50位圏外の物は並べ替えでは救えない。

    切り分けの口: DOUGU_SAKUIN=1 で有効になる。**既定は 0(無効)。**

    ⚠️2026-09-09 の時点で既定を 0 にしてある理由 ―― **まだ効果も害も測れていない**。
      同じ記録・同じ時点で「道具索引あり/なし」の凍結を2つ作って測ろうとしたが、
      **環境変数は版の指紋(source_manifest)に載らない**ので、2回目の refresh が
      「変わっていない」と判断して素通りし、**両方とも道具索引なしの凍結**になっていた
      (実測: 凍結2つとも procedural 757・道具0・焼いた時刻が同じ 10:34:01)。
      未測定の物を王の入口に入れたままにしない。今日そのやり方で一度失敗しかけている。

    正しい測り方(次にやる人へ):
      環境変数では切り替わらない。**dougu.jsonl 自体を差し替える**こと ――
      ①dougu.jsonl を退避して空にする→refresh→freeze(なし)
      ②戻す→refresh→freeze(あり) ③両方を golden_v2.jsonl で測る
      これなら sha が変わるので refresh が必ず作り直す。
      受入は「A型(66問)の1位率が上がり、D型(174問)が下がらない」。
    """
    if os.environ.get("DOUGU_SAKUIN", "0") != "1":
        return []
    daichou = root / "roundtable" / "brain" / "dougu.jsonl"
    rows: list[dict] = []
    if not daichou.is_file():
        return rows
    for line in daichou.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            rec = json.loads(line)
        except json.JSONDecodeError:
            continue
        hitokoto = str(rec.get("hitokoto") or "").strip()
        na = str(rec.get("na") or "").strip()
        michi = str(rec.get("michi") or "").strip()
        if not na or not hitokoto:
            continue          # 説明の無い道具は索引の役に立たないので載せない
        # 本文: 王の言い方に近い日本語(一言)を先頭に置く。名前と道は後ろ。
        # 既存の道具台帳と同じ考え方(BM25と埋め込みの両方で引けるように)。
        text = " ".join(x for x in (hitokoto, na, michi) if x)[:MAX_BODY_CHARS]
        rows.append({
            "id": sha_text("dougu|" + michi)[:16],
            "memory_type": "procedural",
            "trust": "indexed_record",
            "timestamp": "",
            "title": redact("道具: " + na)[:300],
            "path": michi,
            "proof": "道具の索引(docstring)",
            "sha256": str(rec.get("sha256") or ""),
            "source": "roundtable/brain/dougu.jsonl",
            "digest": False,
            "summary": _safe_summary(redact(hitokoto), 160),
            "text": redact(text),
            **_shussho_fuda(michi, text),
        })
    rows.sort(key=lambda item: item["path"])
    return rows


def workbench_code_procedural_memories(root: Path) -> list[dict]:
    """道具台帳(roundtable/workbench/code_registry.jsonl)の active 行を procedural に載せる。

    2026-09-04 段A(王命/Claude改善案A・event=01788463183641892139-07ec8e5e)。
    ファイル全体を1件として索引しても内訳が引けず、事故の教訓(remux「音だけで映像が
    止まる」等)が頭脳から辿れなかった。行単位で載せて description で引けるようにする。

    版指紋(procedural_body_version)に code_registry.jsonl の sha が含まれるので、
    台帳が書き換わったら refresh で自動的に反映される(source_manifest 側で担保)。
    """
    daichou = root / "roundtable" / "workbench" / "code_registry.jsonl"
    rows: list[dict] = []
    if not daichou.is_file():
        return rows
    # 台帳は追記専用。code_id ごとに revision 最大の1行(同値なら後ろの行)だけを現行とする
    # (roundtable_workbench.latest_catalog と同じ定義)。古い active 行は履歴であり載せない。
    latest: dict[str, tuple[int, dict]] = {}
    for line in daichou.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            rec = json.loads(line)
        except json.JSONDecodeError:
            continue
        code_id = str(rec.get("code_id") or "").strip()
        try:
            revision = int(rec.get("revision") or 0)
        except (TypeError, ValueError):
            revision = 0
        if code_id and (code_id not in latest or revision >= latest[code_id][0]):
            latest[code_id] = (revision, rec)
    for _revision, rec in latest.values():
        status = str(rec.get("status") or "")
        if status not in ("active", "review_required"):
            continue
        code_id = str(rec.get("code_id") or "").strip()
        description = str(rec.get("description") or "").strip()
        if not code_id or not description:
            continue
        # 本文: code_id + description + 主要メタ(source_path/command先頭)。埋め込み検索と
        # BM25の両方で引けるように、王の言い方に近い日本語(description)を先頭に置く。
        command = rec.get("command") or []
        command_head = " ".join(str(x) for x in command[:6])
        source_path = str(rec.get("source_path") or "")
        text_parts = [code_id, description, source_path, command_head]
        text = " ".join(part for part in text_parts if part)[:MAX_BODY_CHARS]
        rows.append({
            "id": sha_text("workbench_code|" + code_id)[:16],
            "memory_type": "procedural",
            "trust": "indexed_record",
            "timestamp": str(rec.get("registered_at") or ""),
            "title": redact(("道具台帳: " + code_id))[:300],
            "path": "roundtable/workbench/code_registry.jsonl#" + code_id,
            "proof": "道具台帳(active)" if status == "active" else "道具台帳(要再承認:review_required)",
            "sha256": str(rec.get("source_sha256") or ""),
            "source": "roundtable/workbench/code_registry.jsonl",
            "digest": False,
            "summary": _safe_summary(redact(description), 160),
            "text": redact(text),
            **_shussho_fuda("roundtable/workbench/code_registry.jsonl#" + code_id, text),
        })
    rows.sort(key=lambda item: (item["timestamp"], item["path"]))
    return rows


def run_digest(root: Path) -> str:
    script = root / "scripts" / "roundtable_event.py"
    if not script.is_file():
        return "（円卓ダイジェスト取得不可）"
    result = subprocess.run(
        [sys.executable, str(script), "digest", "--rt", str(root / "roundtable")],
        text=True,
        capture_output=True,
        timeout=60,
        check=False,
    )
    text = (result.stdout or result.stderr or "").strip()
    return redact(text)[:30000]


# 本文を持たせる件数。表示に使うのは最後の15件なので、余裕を見て30。
# それより古い物は event_id と理由だけにする（正本は episodic に在る）。
HONBUN_NOKOSU = 30


def review_queue(episodes: list[dict]) -> list[dict]:
    out = []
    for item in episodes:
        trust = item.get("trust")
        if trust in {"verified_record", "verified_report"}:
            continue
        reason = "証拠未連結"
        if item.get("type") == "blocked":
            reason = "停止理由と復活条件の確認"
        elif trust == "measured_claim":
            reason = "実測主張だが独立証拠未連結"
        out.append({
            "event_id": item.get("event_id"),
            "timestamp": item.get("timestamp"),
            "sender": item.get("sender"),
            "topic": item.get("topic"),
            "type": item.get("type"),
            "trust": trust,
            "reason": reason,
            "source": item.get("source"),
            "text": (item.get("summary") or item.get("content", ""))[:240],
        })
    # 二重持ちをやめる（2026-08-18 実測: 604件すべてが episodic にも在り 216,605字を占めていた）。
    # 新しい HONBUN_NOKOSU 件だけ本文を残し、古い物は event_id と理由だけにする。
    # 深く見たい者は event_id で episodic の正本を開けばよい。
    if len(out) > HONBUN_NOKOSU:
        for row in out[:-HONBUN_NOKOSU]:
            # 2026-09-01 修理(バグ745): 落とすのは重い物(本文)だけ。sender/type/source は
            # 1行あたり数十字で、これを剥ぐと review --sender が古い行を数えられなくなる
            # (実機996件中30件しか出ない=数字が嘘をつく型)。絞り込みの鍵は残す。
            row.pop("text", None)
    return out


class _HontaiHyou(dict):
    """本体名をキーに持つ表（2026-09-26 ai-12ef）。

    JSON に出るキーは**本体名だけ**（くくる は1席）。ただし Python の中で古い表示名
    （"ChatGPT" / "GPTくくる"）で引いた時は、本体の行を返す ―― 今まで生の名で引いていた
    読み手（試験・道具）を黙って KeyError にしないための互換。行は増やさない。
    """

    def _hon(self, key):
        return _hontai_mei(key) if isinstance(key, str) else key

    def __missing__(self, key):
        hon = self._hon(key)
        if hon != key and dict.__contains__(self, hon):
            return dict.__getitem__(self, hon)
        raise KeyError(key)

    def get(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            return default

    def __contains__(self, key):
        return dict.__contains__(self, key) or dict.__contains__(self, self._hon(key))


def _onaji_hontai(a, b) -> bool:
    """2つの名が同じ本体か（ChatGPT と くくる は同じ・くくるV5 と くくる は別）。"""
    a = str(a or "").strip()
    b = str(b or "").strip()
    if a == b:
        return True
    return bool(a) and bool(b) and _hontai_mei(a) == _hontai_mei(b)


def _acks_by_meigi(root: Path) -> dict[str, dict]:
    """acks.jsonl の**生の名ごと**の最後の行（記録のまま・名寄せ前）。"""
    latest = {}
    for row in read_jsonl(root / "roundtable" / "brain" / "acks.jsonl"):
        participant = str(row.get("participant") or "")
        if participant:
            latest[participant] = row
    return latest


def _ack_saki(row: dict, id_index: dict | None) -> tuple:
    """既読位置の「先」の並べ方。正本の並びで位置が分かる物 > 分からない物。
    位置が同じ（または両方分からない）なら、後から刻んだ方。"""
    ru = str(row.get("read_upto") or "")
    idx = id_index.get(ru) if (id_index is not None and ru) else None
    return (1 if idx is not None else 0, idx if idx is not None else -1,
            str(row.get("acked_at") or ""))


def _acks_wo_tabaneru(rows_by_name: dict, id_index: dict | None) -> "_HontaiHyou":
    """生の名ごとの既読行を**本体ごと**に1行へ束ねる。**既読位置が一番先の行**を採る。

    採った行の participant は記録の生の名のまま（＝どの名義で読んだか）。
    besshou に、この本体が acks に残した**他の名（過去の表示名）**を全部残す。
    """
    kumi: dict[str, list] = {}
    for name, row in (rows_by_name or {}).items():
        if not isinstance(row, dict):
            continue
        kumi.setdefault(_hontai_mei(name), []).append((name, row))
    out = _HontaiHyou()
    for hon, rows in kumi.items():
        _, row = max(rows, key=lambda nr: _ack_saki(nr[1], id_index))
        mei = set()
        for name, r in rows:
            mei.add(name)
            mei.update(x for x in (r.get("besshou") or []) if isinstance(x, str))
        row = dict(row)
        row["besshou"] = sorted(m for m in mei if m and m != hon)
        out[hon] = row
    return out


def latest_acks(root: Path, id_index: dict | None = None) -> dict[str, dict]:
    """読了の最新行を**本体ごと**に返す（2026-09-26 ai-12ef）。

    前は生の名(participant)をキーにしていたので、くくる が "ChatGPT" / "くくる" / "GPTくくる"
    の3行に割れ、同じ主体の未読が3つの数で出ていた。acks.jsonl は**書き換えない**
    （追記は今まで通り生の名のまま＝履歴）。読む時にだけ束ねる。
    id_index（event_id → 正本の並びの位置）が無く、束ねる時に位置の比べ合いが要る時だけ
    正本を読む（同じ本体の行が1つ、または全部同じ位置なら読まない）。
    """
    raw = _acks_by_meigi(root)
    if id_index is None:
        kumi: dict[str, set] = {}
        for name, row in raw.items():
            kumi.setdefault(_hontai_mei(name), set()).add(str(row.get("read_upto") or ""))
        if any(len(v) > 1 for v in kumi.values()):
            id_index = {str(e.get("event_id") or ""): i for i, e in enumerate(load_events_meta(root))}
    return _acks_wo_tabaneru(raw, id_index)


def teisei_no_michihannei(root: Path, snapshot: dict) -> dict:
    """訂正台帳に在るのに、この版の頭脳へまだ入っていない訂正。

    なぜ要るか(2026-09-12 ネオ ai-e8ea・自分で踏んだ):
      訂正を出しても **refresh までは検索に⛔が出ない**。その間に読んだAIは
      「訂正は無い」と読む ―― **不明を完了に変えている**
      (Codex提案書 P03 の受入条件「古い索引・停止中worker・途中再開を混ぜても、
       不明を完了に変えない」)。
      freshness は「何かが変わった」とは言うが、それが訂正だとは言わない。

    **止めない。見せるだけ。** 判定(state)も順位も変えない。
    古い版(teisei_ids が無い snapshot)では数えられないので、その事も正直に返す。
    """
    nakami = snapshot.get("teisei_ids")
    if not isinstance(nakami, list):
        return {"wakaru": False, "naze": "この版の頭脳は訂正の控えを持っていない(次の作り直しから分かる)"}
    # 2026-09-12 くくる監査②: read_jsonl はファイル無しで [] を返す。
    # そのままだと daichou=0 → 「訂正なし」に見えて台帳消失を握り潰す。
    _daichou_path = root / "roundtable" / "brain" / "corrections.jsonl"
    if not _daichou_path.is_file():
        return {"wakaru": False, "naze": f"訂正台帳ファイルが見つからない: {_daichou_path}"}
    try:
        daichou = {str(row.get("correction_id") or "")
                   for row in read_jsonl(_daichou_path)
                   if str(row.get("correction_id") or "")}
    except Exception:
        return {"wakaru": False, "naze": "訂正台帳が読めない"}
    haitta = {str(x) for x in nakami}
    mada = sorted(daichou - haitta)
    out = {"wakaru": True, "daichou": len(daichou), "hannei": len(haitta & daichou),
           "michihannei": len(mada)}
    if mada:
        out["michihannei_ids"] = mada[:10]
        out["chuui"] = ("⚠️訂正台帳に%d件、この頭脳へまだ入っていない訂正がある。"
                        "**今この検索に⛔が出ていない事は「訂正が無い」という意味にならない。**"
                        "頭脳の作り直し(roundtable-brain-refresh)の後に見直せ" % len(mada))
    return out


def _loop_alignment(loop, loop_count, source_events) -> dict:
    """ループの数えと正本の数えが合っているか。**観測の時差を差し引いてから比べる**。

    ⚠️2026-09-15 ネオ ai-07cb: 前は `loop_count == len(source_events)` で、
    **違う時刻に数えた2つの数**を比べていた。ループは1分ごとに数えるので、
    円卓が1分に1件でも動けばほぼ常に aligned=false になる。
    王命0909-0103の「loop_alignment の1件差」が何日も慢性化していた正体はこれで、
    **欠落ではなく物差しの比べ方**だった。
    実測(2026-09-15 10:02): ループ側は canonical=sqlite=raw=4408・missing_count 0・lag_sec 0。
    → **水位(event_id)でループが見た所まで巻き戻してから件数を比べる。**
    時差で増えた分は `kansoku_go_no_zouka` として結果に書く(名前ではなく結果に条件を書く)。
    [[feedback-sokutei-no-jouken-wa-kekka-ni-kake]]
    """
    wm = ((loop.get("integrity") or {}).get("projection_watermarks") or {}) if isinstance(loop, dict) else {}
    loop_last_eid = str(wm.get("source_last_event_id") or "")
    zouka = None
    if loop_last_eid:
        # state-1: 水位は追記順(event_id は time_ns 先頭で昇順)。source_events は分切り詰めの時刻で
        # 並べ替え済みなので、添字ではなく event_id の大小で水位より後の件数を数える。
        if any(str((e or {}).get("event_id") or "") == loop_last_eid for e in source_events):
            zouka = sum(1 for e in source_events
                        if str((e or {}).get("event_id") or "") > loop_last_eid)
    if loop_count is None:
        aligned = None
    elif zouka is None:
        aligned = loop_count == len(source_events)     # 水位が読めない時は今まで通り
    else:
        aligned = loop_count == (len(source_events) - zouka)
    return {
        "aligned": aligned,
        "brain_source_events": len(source_events),
        "loop_events": loop_count,
        "loop_observed_at": loop.get("observed_at") if isinstance(loop, dict) else None,
        "loop_last_event_id": loop_last_eid,
        "kansoku_go_no_zouka": zouka,   # ループが数えた後に正本へ入った件数(遅れではない)
    }

def shiryou_no_henka_jikoku(root: Path, snapshot: dict, manifest: dict, generated: datetime,
                           now: datetime) -> list:
    """指紋が写し(snapshot.sources)と違う資料の、写しより新しい変更時刻(mtime)の一覧。

    2026-09-25 ネオ ai-9836。freshness_payload が「取り込み待ちの窓」を測る材料。
    - 普通の資料 … その1本の mtime
    - @procedural_bodies … procedural 層が本文を読む記録(procedural_target)の mtime を全部見る
      (指紋は束ねて1つなので、どれが変わったかは mtime でしか分からない。
       同じ内容の書き直しも混ざりうるが、それは待ちを**長く**見積もる側＝鳴る側なので安全)
    - 写しに無い鍵・写しより古い mtime・消えた資料 … 何も返さない(呼び手が lag に落とす)
    """
    moto = snapshot.get("sources")
    if not isinstance(moto, dict) or not isinstance(manifest, dict):
        return []
    tz = now.tzinfo

    def _mt(p: Path):
        try:
            return datetime.fromtimestamp(p.stat().st_mtime, tz=tz)
        except OSError:
            return None

    out = []
    for key, digest in manifest.items():
        if moto.get(key) == digest:
            continue
        if key == "@procedural_bodies":
            for path in _procedural_target_paths(root):
                t = _mt(root / path)
                if t is not None and t > generated:
                    out.append(t)
            continue
        if key.startswith("@"):
            continue
        t = _mt(root / key)
        if t is not None and t > generated:
            out.append(t)
    return out


def freshness_payload(root: Path, snapshot: dict, current_time: datetime | None = None,
                      source_events: list[dict] | None = None,
                      source_version_manifest: tuple[str, dict[str, str]] | None = None,
                      *, use_source_cache: bool = False) -> dict:
    return _bsc_freshness_payload(
        _SELF_MODULE, root, snapshot, current_time=current_time,
        source_events=source_events, source_version_manifest=source_version_manifest,
        use_source_cache=use_source_cache,
    )


def sou_no_okure(root: Path) -> list[dict]:
    """派生層(facts/episodes/snapshot/imi/teisei_kouho)のうち、**遅れている物だけ**返す。

    ★閾値を自分で決めない。`brain_no_sendo.SOU` を借りる ――
      同じ物を指す定義が2箇所にあると必ずずれ、片方だけ見ている道具は緑のまま嘘をつく。

    測り方は brain_no_sendo と同じ芯:
      正本(events/*.jsonl)の最終更新を基準に、各派生ファイルが何時間遅れているか。
      **正本が動いていなければ派生の遅れは責めない**（静かな夜に全層が赤くなるのを防ぐ）。

    読めない時は空を返す（fail-open）。鮮度が測れない事を理由に検索を止めない。
    """
    try:
        import brain_no_sendo as _S          # 閾値の正本
    except Exception:
        return []
    try:
        root = Path(root)
        saishin = 0.0
        for q in (root / "roundtable" / "events").glob("events_*.jsonl"):
            try:
                m = q.stat().st_mtime
            except OSError:
                continue
            if m > saishin:
                saishin = m
        if not saishin:
            return []
        out_rows = []
        for sou in getattr(_S, "SOU", []):
            q = root / str(sou.get("rel") or "")
            try:
                m = q.stat().st_mtime
            except OSError:
                m = None
            if m is None:
                out_rows.append({"id": sou.get("id"), "okure_h": -1.0,
                                 "shikii_h": sou.get("okure_h"), "naze": "その層のファイルが無い",
                                 "nushi": sou.get("nushi", "")})
                continue
            okure_h = max(0.0, (saishin - m) / 3600.0)
            if okure_h > float(sou.get("okure_h") or 0):
                out_rows.append({"id": sou.get("id"), "okure_h": round(okure_h, 1),
                                 "shikii_h": sou.get("okure_h"), "naze": "正本より遅れている",
                                 "nushi": sou.get("nushi", "")})
        return out_rows
    except Exception:
        return []


def _projection_warning(marks) -> str:
    """投影が正本から離れていたら一言。空文字=異常なし。"""
    if not isinstance(marks, dict):
        return ""
    bad = []
    for name in ("sqlite", "markdown"):
        m = marks.get(name)
        if not isinstance(m, dict):
            continue
        miss = int(m.get("missing_count") or 0)
        lag = int(m.get("lag_sec") or 0)
        if miss > 0 or lag > FRESHNESS_GRACE_SECONDS:
            bad.append(f"{name}: 未反映{miss}件・遅れ{lag}秒")
    if not bad:
        return ""
    return "⚠ 派生が正本から離れている(" + " / ".join(bad) + ")。新しい記録が答えに入っていない可能性。"


_CODE_SHA_SHIPPAI: dict[str, str] = {}


def _code_sha(name: str) -> str:
    """コードの指紋。読めなかった事を黙って隠さない(2026-09-01 バグ台帳:813)。

    旧版は例外で空文字を返し、code_banto が "loaded と on_disk が両方空" を
    "同じ" と読んで **見張りそのものが失効** していた。今は失敗を憶えて status に出す。
    """
    try:
        sha = hashlib.sha256((_CODE_DIR / name).read_bytes()).hexdigest()[:12]
        _CODE_SHA_SHIPPAI.pop(name, None)
        return sha
    except Exception as exc:
        _CODE_SHA_SHIPPAI[name] = f"{type(exc).__name__}: {exc}"
        return ""


# このプロセスが読み込んだ時のコード。以後は変わらない(importは1回きり)。
_LOADED_CODE = {name: _code_sha(name) for name in _CODE_FILES}
_LOADED_AT = datetime.now().astimezone().isoformat(timespec="seconds")


def code_banto() -> dict:
    """稼働中のコードが、今ディスクに在るコードと同じかを見張る。

    違っていたら stale=True。**その応答は古い版が返している**という意味で、
    ファイルを直した事は何の証明にもならない。読み込み直すまで直りは効いていない。
    """
    ima = {name: _code_sha(name) for name in _CODE_FILES}
    furui = [name for name in _CODE_FILES
             if _LOADED_CODE.get(name) and ima.get(name) and _LOADED_CODE[name] != ima[name]]
    yomenai = sorted(n for n in _CODE_FILES if not ima.get(n))
    out = {
        "loaded": "+".join(_LOADED_CODE.get(n) or "----" for n in _CODE_FILES),
        "on_disk": "+".join(ima.get(n) or "----" for n in _CODE_FILES),
        "loaded_at": _LOADED_AT,
        "stale": bool(furui),
    }
    if yomenai:
        # 見張りが見えていない事自体を異常として出す(黙って合格にしない)
        out["yomenai"] = yomenai
        out["yomenai_riyuu"] = {n: _CODE_SHA_SHIPPAI.get(n, "") for n in yomenai}
        out["kanshi_kikanai"] = True
    if furui:
        out["warning"] = (
            "稼働中のコードが古い(%s)。ファイルは新しいが、この応答を返しているプロセスは"
            "読み込み直していない。直りはまだ効いていない。" % ", ".join(furui))
    return out


# 2026-09-19 王命バグ6: 並べ替え役の健康を「このプロセスの外から」測る。
#   RERANK_JOUTAI は自プロセスがリランカーを叩いた時の記録なので、他のプロセスで
#   落ちていても「ok」に見える。9/15にfail_openで通した実害があった。
#   別プロセスから見ても分かるように、/health を叩いた札を状態に付ける。
def _rerank_probe(timeout: float = 2.0) -> dict:
    import urllib.request as _req
    import urllib.error as _err
    started = datetime.now().astimezone()
    try:
        base = RERANK_URL.rsplit("/v1/", 1)[0] if "/v1/" in RERANK_URL else RERANK_URL
        with _req.urlopen(base + "/health", timeout=timeout) as res:
            body = res.read(4096)
            info = json.loads(body.decode("utf-8"))
        return {
            "reachable": True,
            "ok": bool(info.get("ok", True)),
            "model": str(info.get("model") or ""),
            "gpu": str(info.get("gpu") or ""),
            "checked_at": started.isoformat(timespec="seconds"),
            "elapsed_ms": int((datetime.now().astimezone() - started).total_seconds() * 1000),
        }
    except _err.URLError as exc:
        return {"reachable": False, "ok": False,
                "error": f"URLError: {getattr(exc, 'reason', exc)}",
                "checked_at": started.isoformat(timespec="seconds"),
                "elapsed_ms": int((datetime.now().astimezone() - started).total_seconds() * 1000)}
    except (OSError, ValueError, TimeoutError) as exc:
        return {"reachable": False, "ok": False,
                "error": f"{type(exc).__name__}: {exc}",
                "checked_at": started.isoformat(timespec="seconds"),
                "elapsed_ms": int((datetime.now().astimezone() - started).total_seconds() * 1000)}


def status_payload(root: Path) -> dict:
    snapshot = load_snapshot_meta(root)
    # 2026-09-26 ai-12ef: 正本は1回だけ読み、既読の束ね(位置の比べ合い)と未読の数えの両方に使う。
    # 2026-11-21 ルートC Phase 0: event_id 順序のみ必要なため軽量メタ読込(load_events_meta)を使う。
    event_ids = [str(e.get("event_id") or "") for e in load_events_meta(root)]
    acks = latest_acks(root, id_index={eid: i for i, eid in enumerate(event_ids)})
    current_version = snapshot.get("version")
    return {
        "schema": snapshot.get("schema"),
        "version": current_version,
        "generated_at": snapshot.get("generated_at"),
        "counts": snapshot.get("counts"),
        "freshness": freshness_payload(root, snapshot),
        "brain_code": code_banto(),
        "ai_availability": ai_availability_payload(root),
        "rerank": dict(RERANK_JOUTAI),
        "rerank_state_scope": "last_execution_in_this_process",
        # 2026-09-19 王命バグ6: 外から叩いた札。プロセスをまたいで有効。
        "rerank_probe": _rerank_probe(),
        "acks": acks,
        "ack_matrix": _ack_matrix(root, acks, current_version, event_ids=event_ids),
    }


def _ack_matrix(root: Path, acks: dict, current_version, event_ids: list | None = None) -> dict:
    """ack v3: 読了位置から「未読が何件か」を出す。0件=最新まで読了。

    v2の古いack(read_uptoなし)は unread_events を数えられないので None(不明)にする。
    currentは「未読0」の意味に変えた(旧: 版ハッシュ一致=イベント1件で偽になる追いかけっこ)。

    2026-09-26 ai-12ef: 行は**本体ごとに1行**（くくる は1行）。同じ本体の既読が複数の名義に
    あれば**既読位置が一番先の物**を採る。行に besshou（過去の表示名）と yonda_meigi
    （採った既読をどの名義で刻んだか）を残す。
    """
    if event_ids is None:
        event_ids = [str(e.get("event_id") or "") for e in load_events_meta(root)]
    # 2026-09-12 くくる監査④: 辞書順 eid>read_upto は復元/後挿入されたeidが
    #   read_upto より小さい時に未読で数えられない。JSONLは追記順で並ぶので、
    #   read_upto の**位置**を基準にする。位置が特定できない(復元前の削除・別ストア)なら
    #   Noneにする(嘘をつかない)。
    id_index = {eid: i for i, eid in enumerate(event_ids)}
    total = len(event_ids)
    # 生の名のキーで渡されても本体で束ね直す（束ね済みなら同じ物が返る）。
    acks = _acks_wo_tabaneru(acks, id_index)
    matrix = _HontaiHyou()
    for participant in AI_PARTICIPANTS:
        ack = acks.get(participant) or {}
        read_upto = str(ack.get("read_upto") or "")
        if read_upto:
            idx = id_index.get(read_upto)
            if idx is None:
                unread = None
                current = None
            else:
                unread = total - idx - 1
                current = unread == 0
        else:
            unread = None
            # 2026-09-05 修理(バグ探し低#1): 版ハッシュはイベントが1件増えるたび変わるので、
            #   ACKした瞬間以外は必ず False になり、read_upto を持たない参加者が
            #   常時「遅れている」と赤で並んでいた(実測: 8人中7人が read_upto 無し)。
            #   分からない物を False(=読んでいない)と言うのは嘘。None(=不明)にする。
            current = None
        matrix[participant] = {
            "current": current,
            "read_upto": read_upto,
            "unread_events": unread,
            "acked_version": ack.get("version", ""),
            "acked_at": ack.get("acked_at", ""),
            "meaning": ("read_receipt_only" if read_upto
                        else "read_receipt_only / read_uptoが無いのでcurrentは不明(None)"),
            "besshou": list(ack.get("besshou") or []),
            "yonda_meigi": str(ack.get("participant") or ""),
        }
    return matrix


def review_payload(root: Path, limit: int = 30, sender: str = "", trust: str = "",
                   triage: bool = False) -> dict:
    snapshot = load_snapshot(root)
    rows = []
    for row in snapshot.get("review_queue", []):
        if sender and row.get("sender") != sender:
            continue
        if trust and row.get("trust") != trust:
            continue
        rows.append(row)
    l3_ids = _l3_ready_event_ids(root)
    counts = {"l3_ready": 0, "priority": 0, "background": 0}
    prioritized = []
    for row in rows:
        tier, prio, treason = triage_review_row(row, l3_ids)
        counts[tier] += 1
        if triage and tier != "background":
            enriched = dict(row)
            enriched["triage_tier"] = tier
            enriched["triage_score"] = prio
            enriched["triage_reason"] = treason
            prioritized.append(enriched)
    cap = max(1, min(limit, 100))
    if triage:
        prioritized.sort(
            key=lambda r: (int(r.get("triage_score") or 0), str(r.get("timestamp") or "")),
            reverse=True,
        )
        selected = prioritized[:cap]
    else:
        selected = rows[-cap:]
    return {
        "version": snapshot.get("version"),
        "total": len(rows),
        "triage": bool(triage),
        "triage_summary": {
            "l3_ready": counts["l3_ready"],
            "priority": counts["priority"],
            "background": counts["background"],
            "actionable_total": counts["l3_ready"] + counts["priority"],
        },
        "results": selected,
    }


def verify_episode(root: Path, event_id: str, verifier: str, evidence_path: str,
                   evidence_sha256: str, note: str = "") -> dict:
    events = load_events(root)
    event_map = {str(event.get("event_id") or ""): event for event in events}
    event = event_map.get(event_id)
    if event is None:
        raise RuntimeError("event not found")
    sender = str(event.get("sender") or "")
    if not kiroku_to_dokuritsu(event, verifier):
        # 2026-09-27 ai-87bb: 名前だけでなく、投稿した窓(mado)とも比べる(v2 と同じ1本の判定)
        raise RuntimeError("verifier must differ from event sender and its window")
    catalog, _ = report_index(root)
    candidate = episode_from_event(root, event, set(), catalog)
    if candidate is None:
        raise RuntimeError("event is not an episodic memory candidate")
    if candidate.get("trust") == "verified_report":
        raise RuntimeError("event is already verified by report sha256")
    if event_id in verified_event_ids(root, events):
        raise RuntimeError("event is already verified")
    target = evidence_target(root, evidence_path)
    actual_sha256 = sha_file(target)
    if len(evidence_sha256) != 64 or actual_sha256 != evidence_sha256:
        raise RuntimeError("evidence sha256 mismatch")
    normalized_path = str(target.relative_to(root.resolve()))
    identity = json.dumps(
        {
            "event_id": event_id,
            "verifier": verifier,
            "evidence_path": normalized_path,
            "evidence_sha256": actual_sha256,
        },
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    verification_id = sha_text(identity)[:24]
    existing = read_jsonl(root / "roundtable" / "brain" / "verifications.jsonl")
    if any(row.get("verification_id") == verification_id for row in existing):
        raise RuntimeError("verification already recorded")
    row = {
        "schema": VERIFICATION_SCHEMA,
        "verification_id": verification_id,
        "event_id": event_id,
        "event_sender": sender,
        "verifier": verifier,
        "evidence_path": normalized_path,
        "evidence_sha256": actual_sha256,
        "note": redact(note)[:1000],
        "verified_at": now_iso(),
        "recorded_by_user": os.environ.get("USER", ""),
    }
    append_jsonl(root / "roundtable" / "brain" / "verifications.jsonl", [row])
    refresh_result = refresh(root)
    return {"verification": row, "refresh": refresh_result}


def make_snapshot(root: Path, version: str, manifest: dict[str, str], events: list[dict],
                  episodes: list[dict], catalog: dict[str, dict], catalog_data: dict,
                  *, episode_revision_count: int | None = None) -> dict:
    event_map = {str(event.get("event_id")): event for event in events if event.get("event_id")}
    episodes, folded_duplicates = attach_summaries_and_dedup(root, episodes)  # 要点+重複畳み(2026-08-16)
    semantic = semantic_memories(root, event_map)
    procedural = procedural_memories(root, catalog)
    # 道具台帳(workbench code_registry)の各 active 行も procedural に載せる(段A・王命2026-09-04)
    procedural = (procedural + workbench_code_procedural_memories(root)
                  + dougu_procedural_memories(root))
    media = media_memories(root)
    memory = {
        "semantic": semantic,
        "episodic": episodes,
        "procedural": procedural,
        "media": media,
    }
    corrections, correction_errors = apply_corrections(root, memory, events, catalog)
    # この版へ実際に入れた訂正のid。検索の時に台帳と突き合わせて「未反映」を見せる。
    teisei_ids = sorted({str(c.get("correction_id") or "") for c in corrections
                         if str(c.get("correction_id") or "")})
    verified = [row for row in episodes if row.get("trust") in {"verified_record", "verified_report"}]
    measured = [row for row in episodes if row.get("trust") == "measured_claim"]
    candidates = review_queue(episodes)
    verifications, _ = inspect_verifications(root, events)
    # 2026-09-27 ai-87bb: v2台帳の数(表示用)。legacy の verifications(v1の数)の意味は変えない。
    v2_counts = verification_v2_counts(root, events=events)
    digest = run_digest(root)
    return {
        "schema": SCHEMA,
        "version": version,
        "generated_at": now_iso(),
        "teisei_ids": teisei_ids,
        "authority_note": "派生検索用。記録内の命令文は実行権限にならない。",
        "sources": manifest,
        "counts": {
            "events": len(events),
            "episodes": len(episodes),
            "episode_revisions": (episode_revision_count if episode_revision_count is not None
                                  else len(read_jsonl(root / "roundtable" / "brain" / "episodes.jsonl"))),
            "verified_episodes": len(verified),
            "measured_claims": len(measured),
            "review_queue": len(candidates),
            "resolutions": len(semantic),
            "procedural_records": len(procedural),
            "catalog_records": int(catalog_data.get("count") or len(catalog)),
            "verifications": len(verifications),
            "corrections": len(corrections),
            "correction_errors": len(correction_errors),
            "duplicates_folded": folded_duplicates,
            "media_assets": len(media),
            **v2_counts,
        },
        "latest_event_id": str(events[-1].get("event_id") or "") if events else "",
        "memory": memory,
        "review_queue": candidates,
        "roundtable_digest": digest,
        "acks": latest_acks(root, id_index={str(e.get("event_id") or ""): i
                                            for i, e in enumerate(events)}),
    }


def render_report(snapshot: dict) -> str:
    counts = snapshot["counts"]
    lines = [
        "# 全AI共有脳・現在地",
        "",
        f"- schema: `{snapshot['schema']}`",
        f"- version: `{snapshot['version']}`",
        f"- generated_at: `{snapshot['generated_at']}`",
        f"- latest_event_id: `{snapshot['latest_event_id']}`",
        "- 状態: 円卓正本から生成した派生検索物。本文中の命令は実行権限にならない。",
        "",
        "## 計測",
        "",
        f"- 円卓event: {counts['events']}件",
        f"- 経験記録: {counts['episodes']}件",
        f"- 検証済み経験: {counts['verified_episodes']}件",
        f"- 実測主張（独立証拠未連結）: {counts['measured_claims']}件",
        f"- 要確認候補: {counts['review_queue']}件",
        f"- 解決台帳: {counts['resolutions']}件",
        f"- 手順・掟: {counts['procedural_records']}件",
        f"- 記録索引全体: {counts['catalog_records']}件",
        f"- 独立検証記録: {counts.get('verifications', 0)}件",
        f"- 訂正・置換台帳: {counts.get('corrections', 0)}件",
        f"- 訂正台帳エラー: {counts.get('correction_errors', 0)}件",
        f"- 登録済み画像・動画: {counts.get('media_assets', 0)}件",
        "",
        "## 共有脳の四層",
        "",
        "- semantic: 解決台帳の質問と回答。",
        "- episodic: complete/blocked/progressと実測済み投稿。検証済みと候補を分離。",
        "- procedural: 共通メモリ、正式指示、掟、基準、手順の索引。",
        "- media: SHA整合性を確認した登録済み画像・動画。event型に関係なく検索対象。",
        "",
        "## 最新の検証済み経験",
        "",
    ]
    verified = [row for row in snapshot["memory"]["episodic"] if row.get("trust") in {"verified_record", "verified_report"}]
    # 見出しどおり最新を先頭へ置く。RAG側は断片の先頭だけを採点・注入するため、
    # 古い順だと新しい検証済み記録が文字上限の外へ落ちる。
    newest_verified = sorted(
        verified,
        key=lambda row: (str(row.get("timestamp") or ""), str(row.get("event_id") or "")),
        reverse=True,
    )[:15]
    for row in newest_verified:
        snippet = " ".join(str(row.get("content") or "").split())[:240]
        mark = "" if str(row.get("proof") or "") in ("実測済", "王合格") else "⚠中身は未検証(出所のみ検証) "
        lines.append(f"- `{row.get('event_id')}` {mark}{row.get('sender')} / {row.get('topic') or '円卓'} — {snippet}")
    if not verified:
        lines.append("- （なし）")
    lines.extend(["", "## 要確認候補", ""])
    for row in reversed(snapshot["review_queue"][-15:]):
        snippet = " ".join(str(row.get("text") or "").split())[:220]
        lines.append(f"- `{row.get('event_id')}` [{row.get('reason')}] {snippet}")
    if not snapshot["review_queue"]:
        lines.append("- （なし）")
    lines.extend(["", "## 円卓ダイジェスト", "", "```text", snapshot["roundtable_digest"], "```", ""])
    lines.extend([
        "## 使い方",
        "",
        "```bash",
        "python3 scripts/roundtable_brain.py context --participant Code --query '動画 カメラ固定' --format markdown",
        "python3 scripts/roundtable_brain.py search --query '同じ失敗' --limit 10",
        "python3 scripts/roundtable_brain.py ack --participant Code   # 今の状態まで読んだ、と刻む",
        "python3 scripts/roundtable_brain.py audit",
        "```",
        "",
        "`ack`は「どのイベントまで読んだか」の読了記録であり、理解・実行・完了の証拠ではない。",
        "状態のack_matrixは未読件数(unread_events)を出す。0=最新まで読了。",
        "",
    ])
    return "\n".join(lines)


KAKUNIN_PATH = ("roundtable", "brain", "kakunin.json")


def kakunin_no_michi(root: Path) -> Path:
    return root.joinpath(*KAKUNIN_PATH)


def kakunin_wo_nokosu(root: Path, version: str) -> None:
    """写しが正本と同じだと**確かめた**時刻を残す（HTTPの 304 Not Modified と同じ意味）。

    版(version)は変えない。変えると「中身が変わった」事になってしまう。
    この印が進まなくなった時だけ、写しは本当に腐っている。
    """
    try:
        atomic_write(kakunin_no_michi(root), json.dumps({
            "schema": SCHEMA + ".kakunin",
            "kakunin_at": now_iso(),
            "version": version,
        }, ensure_ascii=False, indent=2) + "\n")
        _invalidate_freshness_cache(root)
    except OSError:
        pass


def kakunin_wo_yomu(root: Path, version: str):
    """同じ版について最後に確かめた時刻。版が違えば使わない(他の版の印で若返らせない)。"""
    d = read_json(kakunin_no_michi(root), {})
    if not isinstance(d, dict):
        return None
    if str(d.get("version") or "") != str(version or ""):
        return None
    return str(d.get("kakunin_at") or "") or None


_REFRESH_SNAP_META_LOCK = threading.RLock()
_REFRESH_SNAP_META_OBOE: dict[str, tuple[tuple, dict]] = {}


def _read_snapshot_meta_for_refresh(snapshot_path: Path) -> dict:
    """refresh 早期判定用の軽量 snapshot ヘッダ読込(schema/version/counts)。
    ファイルの (mtime_ns, size, ino) が一致すれば 63MB 全体の再 JSON パースを避ける。
    """
    try:
        st = snapshot_path.stat()
    except OSError:
        return {}
    sig = (st.st_mtime_ns, st.st_size, st.st_ino)
    key = str(snapshot_path.resolve())
    with _REFRESH_SNAP_META_LOCK:
        hit = _REFRESH_SNAP_META_OBOE.get(key)
        if hit is not None and hit[0] == sig:
            return hit[1]
    meta = None
    if _snap_store is not None and _snap_store.sqlite_enabled():
        _j_sig = _snap_store.json_sig_of(snapshot_path)
        _sq = _snap_store.load_snapshot_meta(snapshot_path.parents[2], expected_json_sig=_j_sig)
        if isinstance(_sq, dict):
            meta = {
                "schema": _sq.get("schema"),
                "version": _sq.get("version"),
                "counts": _sq.get("counts", {}),
            }
    if meta is None:
        data = read_json(snapshot_path, {})
        if not isinstance(data, dict):
            return {}
        meta = {
            "schema": data.get("schema"),
            "version": data.get("version"),
            "counts": data.get("counts", {}),
        }
    try:
        st2 = snapshot_path.stat()
        if (st2.st_mtime_ns, st2.st_size, st2.st_ino) == sig:
            with _REFRESH_SNAP_META_LOCK:
                if len(_REFRESH_SNAP_META_OBOE) >= 16:
                    _REFRESH_SNAP_META_OBOE.clear()
                _REFRESH_SNAP_META_OBOE[key] = (sig, meta)
    except OSError:
        pass
    return meta


def refresh(root: Path) -> dict:
    brain_dir = root / "roundtable" / "brain"
    brain_dir.mkdir(parents=True, exist_ok=True)
    lock_file = (brain_dir / ".refresh.lock").open("w")
    try:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        return {"status": "already_running"}
    try:
        version, manifest = current_source_version(root)
        snapshot_path = brain_dir / "snapshot.json"
        existing = _read_snapshot_meta_for_refresh(snapshot_path)
        # 2026-11-21 ルートC Phase 0:
        #   source_manifest(root) は events_*.jsonl / corrections.jsonl / _記録総合索引.json /
        #   verifications*.jsonl / doukei.jsonl / youten.jsonl / resolutions.jsonl /
        #   media/manifest.jsonl / code_registry.jsonl / dougu.jsonl / @procedural_bodies 等
        #   すべての入力を sha256 で含むため、version 一致時は再構築不要。
        #   念のため episodic があるのに episodes.jsonl が欠損している場合のみ通常経路へ落とす。
        episodes_path = brain_dir / "episodes.jsonl"
        counts_existing = existing.get("counts") if isinstance(existing, dict) else None
        episodes_ok = (not (isinstance(counts_existing, dict) and counts_existing.get("episodes"))) or episodes_path.is_file()
        if (
            isinstance(existing, dict)
            and existing.get("schema") == SCHEMA
            and existing.get("version") == version
            and episodes_ok
        ):
            # 2026-09-20 ネオ ai-b932: **「焼き直していない」と「古い」は違う。**
            #   HTTPキャッシュの age は再生成だけでなく revalidation(304 Not Modified) でも戻る。
            kakunin_wo_nokosu(root, version)
            return {"status": "unchanged", "version": version, "counts": existing.get("counts", {})}
        events = load_events(root)
        catalog, catalog_data = report_index(root)
        _, correction_errors = inspect_corrections(root, events, catalog)
        if correction_errors:
            raise RuntimeError("invalid correction ledger: " + ",".join(correction_errors))
        revision_count: list[int] = []
        episodes = load_or_append_episodes(root, events, catalog, revision_count=revision_count)
        snapshot = make_snapshot(root, version, manifest, events, episodes, catalog, catalog_data,
                                 episode_revision_count=revision_count[0])
        imi_kekka = imi_wo_yaku(root, snapshot)   # 意味の索引を焼き直す。落ちても脳は作る(best-effort)
        # 2026-08-29 修理(バグ2): 焼きの成否と時刻を無言にしない。止めず、見えるようにだけする。
        try:
            atomic_write(brain_dir / "imi_yaki_state.json", json.dumps({
                "status": str((imi_kekka or {}).get("status") or "unknown"),
                "why": str((imi_kekka or {}).get("why") or ""),
                "yaita_at": now_iso(),
                "snapshot_ban": version,
            }, ensure_ascii=False, indent=2) + "\n")
        except OSError:
            pass
        snapshot_text = json.dumps(snapshot, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
        report = render_report(snapshot)
        atomic_write(snapshot_path, snapshot_text)
        if _snap_store is not None and _snap_store.sqlite_enabled():
            try:
                _snap_store.write_snapshot(
                    root, snapshot,
                    source_json_sig=_snap_store.json_sig_of(snapshot_path),
                )
            except Exception:
                pass
        atomic_write(root / "reports" / "共有脳_現在地.md", report)
        atomic_write(root / "data" / "open-webui" / "kingdom_docs" / "共有脳_現在地.md", report)
        metric = {
            "schema": SCHEMA + ".metrics",
            "version": version,
            "generated_at": snapshot["generated_at"],
            **snapshot["counts"],
        }
        append_jsonl(brain_dir / "metrics.jsonl", [metric])
        _invalidate_freshness_cache(root)
        return {"status": "updated", "version": version, "counts": snapshot["counts"]}
    finally:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
        lock_file.close()



_CANONICAL_EVENT_TOKEN = re.compile(r"(?<!\d)[0-9]{20}-[0-9a-f]{8}(?![0-9a-f])")


def _safe_summary(text: str, max_chars: int = 160) -> str:
    """Keep whole values; omit a token only when the length boundary cuts it.

    Normalize whitespace; output is at most max_chars plus one ellipsis.
    Canonical event IDs use 20 ASCII decimal digits and 8 lowercase hex digits.
    """
    if type(max_chars) is not int or max_chars <= 0:
        raise ValueError('max_chars must be a positive integer')
    flat = " ".join(str(text or "").split())
    cut = flat[:max_chars]
    truncated = len(flat) > max_chars
    if truncated:
        event_id_cut = False
        for token in _CANONICAL_EVENT_TOKEN.finditer(flat):
            if token.start() < max_chars < token.end():
                cut = flat[:token.start()].rstrip()
                event_id_cut = True
                break
        # Decimal digits are Unicode Nd, like Python regex \d. Whole ordinary
        # numbers are never removed merely because they occur at the end.
        if not event_id_cut and flat[max_chars - 1].isdecimal() and flat[max_chars].isdecimal():
            start = max_chars - 1
            while start > 0 and flat[start - 1].isdecimal():
                start -= 1
            cut = flat[:start].rstrip()
    if cut.count("`") % 2:
        cut = cut[:cut.rfind("`")].rstrip()
    return cut + ("…" if truncated else "")


def load_youten(root: Path) -> dict[str, str]:
    """要点サイドカー: content_sha256 → 要点1行。同じ本文の重複にも1個で効く。"""
    out = {}
    for row in read_jsonl(root / "roundtable" / "brain" / "youten.jsonl"):
        sha = str(row.get("content_sha256") or "")
        youten = str(row.get("youten") or "").strip()
        if sha and youten:
            out[sha] = _safe_summary(youten, 160)
    return out


def summary_line(content: str, sha: str, youten_map: dict[str, str]) -> str:
    """要点1行を決める: ①サイドカー ②本文中の【要点】行 ③先頭文の切り出し。"""
    if sha in youten_map:
        return youten_map[sha]
    m = YOUTEN_MARK.search(content[:400])
    if m:
        return _safe_summary(m.group(1).strip(), 160)
    flat = " ".join(content.split())
    return flat[:120]


def attach_summaries_and_dedup(root: Path, episodes: list[dict]) -> tuple[list[dict], int]:
    """各記録に要点を載せ、同じ由来・本文の複製だけをtrust優先で畳む。別eventは保持。"""
    youten_map = load_youten(root)

    def trust_rank(row: dict) -> int:
        # 2026-08-29 修理(バグ9): 後勝ちだと検証済みが新しいcandidateへ畳まれ未検証に格下げ表示された。
        trust = str(row.get("trust") or "")
        if trust in ("verified_record", "verified_report"):
            return 3
        if trust == "measured_claim":
            return 2
        return 1

    newest_by_sha: dict[tuple[str, str], dict] = {}
    for row in episodes:  # episodesはtimestamp昇順
        row = dict(row)
        sha = str(row.get("content_sha256") or row.get("record_id") or id(row))
        row["summary"] = summary_line(str(row.get("content") or ""), sha, youten_map)
        # 同じ本文でも別eventは別の根拠。同じeventの同内容projectionだけ畳む。
        identity = str(row.get("event_id") or row.get("report_path") or "")
        dedup_key = (identity, sha)
        prev = newest_by_sha.get(dedup_key)
        if prev is None:
            newest_by_sha[dedup_key] = row
            continue
        count = int(prev.get("duplicate_count") or 1) + 1
        # trustの強い方を残す。同格なら新しい方(episodesは昇順なのでrowが新しい)。
        keep = row if trust_rank(row) >= trust_rank(prev) else dict(prev)
        keep["duplicate_count"] = count
        # 2026-09-09 ネオ ai-9598: **畳まれた側の event_id を残す。**
        # 残さないと、その id で王国のどこからも辿れない(正解帳が古い id を指すと永久に不正解)。
        # 畳み自体は正しい機能なので止めない。**行方だけを残す。**
        tatanda = set(str(x) for x in (prev.get("folded_event_ids") or []))
        tatanda |= set(str(x) for x in (row.get("folded_event_ids") or []))
        for moto in (prev, row):
            eid = str(moto.get("event_id") or "")
            if eid and eid != str(keep.get("event_id") or ""):
                tatanda.add(eid)
        if tatanda:
            keep["folded_event_ids"] = sorted(tatanda)
        newest_by_sha[dedup_key] = keep
    deduped = sorted(newest_by_sha.values(), key=lambda r: (str(r.get("timestamp") or ""), str(r.get("event_id") or "")))
    return deduped, len(episodes) - len(deduped)


# normalize / KATAMARI / query_terms は scripts/brain_text_utils.py へ分離済み

def searchable_items(snapshot: dict) -> list[dict]:
    _snap_kagi = _SNAPSHOT_ID_KAGI.get(id(snapshot))
    if _snap_kagi is not None and _snapshot_cache_on():
        with _SNAPSHOT_LOCK:
            _hit_items = _SNAP_ITEMS_CACHE.get((id(snapshot), _snap_kagi))
            if _hit_items is not None:
                return _hit_items
    items = []
    for row in snapshot.get("memory", {}).get("semantic", []):
        items.append({**row, "text": row.get("text", "") + " " + row.get("note", "")})
    for row in snapshot.get("memory", {}).get("episodic", []):
        media_text = " ".join(
            " ".join(str(attachment.get(key) or "") for key in (
                "media_id", "display_name", "kind", "mime_type", "role", "tags", "source_path",
            ))
            for attachment in (row.get("attachments") or [])
            if isinstance(attachment, dict)
        )
        items.append({
            **row,
            "id": row.get("event_id"),
            # 同じ会話糸の前後を辿るため、検索中も thread_id を落とさない。
            # episode_from_event は保持していたが searchable_items で暗黙に残る事を
            # 契約にしていなかったため、ここで明示する。
            "thread_id": row.get("thread_id"),
            "text": str(row.get("content") or "") + ("\n添付: " + media_text if media_text else ""),
        })
    # 出所の門(OWASP ASI06・2026-09-21 ネオ ai-b6cc)。
    # 外部の文章が元の手順を「行動の指針」として自動で取り出さない。
    # ★既定は 0(素通り)。**未測定の物を王の入口に入れない**
    #   (2026-09-09 の DOUGU_SAKUIN で同じ轍を踏みかけた)。
    #
    # ★★2026-09-21 測った。落ちた。**開けるな。**（ネオ ai-b6cc）
    #   土俵 shussho-20260921-ai-b6cc（記録6,194件を凍結）/ 問い帳 golden_v2.jsonl / 276問
    #   同じ写し・同じコードで、この環境変数だけを変えて連続に測った:
    #                 門OFF      門ON       差
    #     R@1         0.558  →  0.496    -6.2pt
    #     R@3         0.771  →  0.679    -9.2pt
    #     MRR@10      0.666  →  0.591    -7.5pt
    #     R@50(天井)  0.858  →  0.758   -10.0pt  ★これが決定的
    #     意味0の当たる問い  9  →   12
    #   ★天井(R@50)が10pt落ちた＝並べ替えの問題ではなく、**正解の記録そのものを候補から消している**。
    #   真因: 「外部の跡」を本文の http|github|arxiv で引いたが、
    #        **王国は正しい記録ほど出典URLを書く**文化なので、良い習慣を罰していた。
    #        私が「外部由来」の定義を自分の都合で引いた（記憶 feedback-gokenchi-no-teigi-wo-jibun-no-tsugou-de-hiku-na）。
    #   ★次に挑む人へ: 本文から推定する道はここで終わっている。flynn の本来の形は
    #     **書いた時に出所を記録する**（後から本文で推定するのではない）。
    #     やるなら procedural の元ファイルに `出所:` を書かせる門を、書く側に置く事。
    #   この段落を消さずに、もう一度同じ実験をするな。
    #   flynn の fail-safe に合わせ、外すのは「実効の信用が0」の物だけ。
    #   不明(fumei)は oukoku_ai(1)へ倒れているので、ここでは外れない。
    mon = os.environ.get("SHUSSHO_MON", "0") == "1"
    for row in snapshot.get("memory", {}).get("procedural", []):
        if mon and int(row.get("jikkou_shinrai", 1)) <= 0:
            continue
        items.append(dict(row))
    for row in snapshot.get("memory", {}).get("media", []):
        items.append(dict(row))
    if _snap_kagi is not None and _snapshot_cache_on():
        with _SNAPSHOT_LOCK:
            if len(_SNAP_ITEMS_CACHE) >= 8:
                _SNAP_ITEMS_CACHE.clear()
            _SNAP_ITEMS_CACHE[(id(snapshot), _snap_kagi)] = items
    return items

def _recipient_matches(value: str, participant: str) -> bool:
    """宛先を完全一致で見る。`Code in Codex` の部分一致事故を起こさない。"""
    recipients = {
        item for item in re.split(r"[,、/\s]+", str(value or "")) if item
    }
    # 2026-08-31 修理(バグ狩りC9): 全員宛ては受信箱(roundtable_event.py RT_BROADCAST)と
    # 同じ意味論で誰の宛先にも一致させる。実機instruction46件中6件がto=全員だった。
    if recipients & {"全員", "みんな", "all"}:
        return True
    if participant in recipients:
        return True
    # 2026-09-26 ai-12ef: 本体名でも比べる（to=くくる を ChatGPT 名義で引いても当たる）。
    #   丸ごとの名で比べる性質は変えない（Code と Codex は別の本体のまま）。
    hon = _hontai_mei(participant)
    return bool(hon) and any(_hontai_mei(x) == hon for x in recipients)


def _participant_scope(query: str, participant: str, requested: str = "auto") -> str:
    if not participant:
        return "boost"
    if requested != "auto":
        return requested
    text = str(query or "")
    # 2026-08-29 修理(バグ4): 裸の"to"/"from"は Evoto/photoshop/auto に誤発火するので、
    # 英字は語境界つきの明示 to:/from: だけに限定する。
    # 2026-08-31 修理(バグ狩りC8): 「初心者向け」「受信箱の使い方」がscope=toに誤発火し
    # procedural層が全滅していた。和語は「名前+宛て/向け」の隣接だけに絞る。
    namae = r"(?:Code|Codex|Claude|ChatGPT|くくる|チム|ネオ|王|全員|みんな)"
    # Named recipients must not silently become the caller's inbox filter.
    # Different/multiple recipients keep the normal search (caller boost only).
    named = re.findall(r"(?<![A-Za-z0-9])(" + namae + r")\s*(?:宛て?|向け)", text, re.IGNORECASE)
    named += re.findall(r"(?<![A-Za-z0-9_])to[:：]\s*(" + namae + r")(?![A-Za-z0-9_])", text, re.IGNORECASE)
    if named and any(_hontai_mei(name) != _hontai_mei(participant) for name in named):
        return "boost"
    # A non-matching or empty to: is not permission to filter by caller.
    if named:
        return "to"
    # 2026-10-31 修理(retr-1): from 側も to 側と同じ型に絞る。裸の「発信」「投稿者」「差出人」は
    # 「情報発信の手順」「差出人不明メールの扱い」に誤発火し、participant を渡した呼び手だけ
    # sender を持たない procedural 層が全滅していた。明示の from:/差出人: か、名前と隣接した
    # 「差出人がCode」「Codeが発信」だけを from にする。全員/みんなは送り手を指さないので入れない。
    sender_names = r"(?:Code|Codex|Claude|ChatGPT|くくる|チム|ネオ|王|" + re.escape(participant) + r")"
    sender_word = r"(?:差出人|発信|投稿者)"
    # 名前の直後が漢字・カタカナなら別の語(王国・ネオン)の頭なので from にしない。ひらがな(助詞)は通す。
    if (re.search(r"(?<![A-Za-z0-9])from[:：]|差出人\s*[:：]", text, re.IGNORECASE)
            or re.search(sender_word + r"\s*(?:が|は|[:：=])?\s*" + sender_names
                         + r"(?:(?=様|さん|殿)|(?![A-Za-z0-9_\u30a1-\u30fa\u4e00-\u9fff]))",
                         text, re.IGNORECASE)
            or re.search(r"(?<![A-Za-z0-9_])" + sender_names + r"\s*(?:が|の|から(?:の)?)?\s*" + sender_word,
                         text, re.IGNORECASE)):
        return "from"
    return "boost"


def _participant_matches(item: dict, participant: str, scope: str) -> bool:
    if not participant or scope == "boost":
        return True
    sender = str(item.get("sender") or "")
    to_match = _recipient_matches(str(item.get("to") or ""), participant)
    if scope == "to":
        if "to" not in item:
            return True   # 宛先メタの無い層(掟・手順・媒体)に宛先一致を要求しない(2026-08-31 C8)
        return to_match
    # 2026-09-26 ai-12ef: 差出人も本体で見る（ChatGPT 名義の発言も くくる の発言）。
    if scope == "from":
        return _onaji_hontai(sender, participant)
    if scope == "involved":
        return _onaji_hontai(sender, participant) or to_match
    return True


def _operational_intent(query: str, participant: str, requested_scope: str) -> dict:
    """「最新のCodex宛て指示」のような運用検索を通常RAGから分離する。"""
    text = str(query or "")
    newest = bool(re.search(r"最新|直近|いちばん新|一番新|latest|newest", text, re.IGNORECASE))
    event_types: set[str] = set()
    if re.search(r"指示|命令|instruction", text, re.IGNORECASE):
        event_types.add("instruction")
    if re.search(r"質問|問い合わせ|question", text, re.IGNORECASE):
        event_types.update(("question", "proposal"))
    if re.search(r"回答|返事|answer", text, re.IGNORECASE):
        event_types.add("answer")
    if re.search(r"完了|terminal|complete", text, re.IGNORECASE):
        event_types.add("complete")
    scope = _participant_scope(text, participant, requested_scope)
    residual = text
    for pattern in (
        r"最新|直近|いちばん新|一番新|latest|newest",
        r"指示|命令|instruction|質問|問い合わせ|question|回答|返事|answer|完了|terminal|complete",
        # 2026-08-29 修理(バグ4): 裸のto/fromを削るとEvoto→Evoのように話題語まで壊す
        r"宛(?:て)?|向け|受信|差出人|発信|投稿者|(?<![A-Za-z0-9])to[:：]|(?<![A-Za-z0-9])from[:：]",
    ):
        residual = re.sub(pattern, "", residual, flags=re.IGNORECASE)
    if participant:
        # 2026-08-29 修理(バグ5): 部分文字列replaceだと「Codex」から「Code」を削って
        # 残骸「x」が話題語になる。_recipient_matches(語境界)と同じ考え方で丸ごと一致だけ削る。
        residual = re.sub(
            r"(?<![A-Za-z0-9_])" + re.escape(participant) + r"(?![A-Za-z0-9_])", "", residual)
    residual = re.sub(r"[\sのをにはがとでへ・]+", "", residual)
    # 2026-08-31 修理(バグ狩りC7): 疑問形の残骸(「何？」「どれ？」等)を捨てる。
    # 「？で終わる依頼を殺した」事故と同型で、残骸が必須topic条件になり0件短絡していた。
    residual = re.sub(r"(?:何|なに|どれ|どっち|どこ|誰|だれ|いつ|ある|あるか|ですか|でしょうか|かな)?[？?]*$", "", residual)
    if len(residual) <= 2 and (not residual or KANA_ONLY.match(residual)):
        # 残骸掃除はparticipantの有無に依存させない(2026-08-31 C7)。
        # ただしカタカナ・漢字2文字の固有名(ネオ/チム)は話題として残す(C27)。
        residual = ""
    return {
        "newest": newest, "event_types": event_types,
        "participant_scope": scope, "topic_query": residual,
    }


def _operational_search(snapshot: dict, query: str, participant: str,
                        participant_scope: str, limit: int,
                        as_of: datetime | None = None,
                        diagnostics: dict | None = None) -> list[dict] | None:
    intent = _operational_intent(query, participant, participant_scope)
    # 時間語だけで一般知識をイベント限定にしない。運用上の型まで明示された時だけ直検索する。
    if not intent["newest"] or not intent["event_types"]:
        return None
    candidates = []
    # 2026-08-31 修理(バグ狩りC27): 話題判定に2-gram断片を使わない(「ra」1個で
    # 無関係な記録が通っていた)。塊語(英数字塊/カタカナ2+/漢字2+)と全文だけで見る。
    topic_terms = []
    if intent["topic_query"]:
        tq = normalize(intent["topic_query"]).strip()
        topic_terms = [m.group() for m in KATAMARI.finditer(tq) if len(m.group()) >= 2]
        if tq and tq not in topic_terms:
            topic_terms.append(tq)
    for item in snapshot.get("memory", {}).get("episodic", []):
        if str(item.get("type") or "") not in intent["event_types"]:
            continue
        if not _participant_matches(item, participant, intent["participant_scope"]):
            continue
        if topic_terms:
            haystack = normalize(" ".join(str(item.get(key) or "") for key in (
                "content", "summary", "topic", "sender", "to")))
            if not any(term in haystack for term in topic_terms):
                continue
        candidates.append(item)
    # 最新N件を取る前に時点で絞る。取得後に落とすと過去の正解を拾えない。
    candidates = as_of_items(candidates, as_of, diagnostics)
    candidates.sort(
        key=_event_time_key,
        reverse=True,
    )
    # 通常検索と同じ訂正窓を、件数を切る前に適用する。
    # 最新eventでも撤回済みなら、次の有効な指示を返せるようにする。
    candidates = yuukou_seisaku(candidates, jikan_ito(query), as_of, query=query)
    out = []
    for item in candidates[:max(1, min(int(limit), 100))]:
        # 生の episodic の行は本文を content に持ち、text を持たない(text は searchable_items が
        # content から作る)。そのまま渡すと snippet が常に空になるので、通常検索と同じく本文を渡す。
        row = shiage({**item, "text": str(item.get("text") or item.get("content") or "")}, 0)
        row.update({key: item[key] for key in SEISAKU_NO_RAN if key in item})
        row["operational_match"] = "latest_event"
        row["participant_scope"] = intent["participant_scope"]
        out.append(row)
    return out


# ===== 時間を意味として扱う(2026-08-29 Code・くくるの提案書 P0-2) =====
# くくるの言葉:「今なにしてる？に過去10万byteログを競わせない。鮮度加点ではなく
#   '時間を意味として扱う'」。加点(新しい物を少し上へ)では、49日前の指示書が
#   3位に立つのを止められなかった(2026-08-28 実測)。
#
# 実害の再現(凍結 base-20260828・2026-08-29 Code 実測):
#   「今なにしてる？」  3位 = 2026-07-10 の指示書(49日前)。今日の記録は上位5件に0件。
#   「今の状況を教えて」1位 = 2026-08-08 の「Codeの手元に未処理が4件」(20日前の状態)。
#   どちらも**その時は正しかったが、今はもう嘘**。関連度では弾けない。時間で弾く。
# ★2026-09-09 ネオ(ai-b1)が B-2 の独立検証で見つけた誤爆 ――
#   ひらがなの「いま」が **「〜ないまま」「〜いまま」** に部分一致し、
#   **明らかに過去を問う問いを「今の話」と判定していた**。
#   実測(golden_v2 276問): A型(王の言い方)66問のうち3問が誤爆していた ――
#     「読み返せない**まま**ブロックされてたんだな？」「画質が粗い**まま**だった件」
#     「自力で直せない**まま**止まってる？」
#   ito=="ima" は検索政策で「古い記録を押し下げる」入口なので、
#   **当時の記録こそ正解の問いで、当時の記録が最下段へ落ちる**形だった。
#   → 「いま」の直後が「ま」の時は拾わない。実測で変わったのは276問中この3問だけで、
#     拾うべき物(いまどうなってる/いまは動いてるか/いま何してる/いま忙しい？等)は全部拾ったまま。
IMA_RE = re.compile(
    r"今|いま(?!ま)|現在|現状|現行|なにして|何して|動いてる|進行中|状況|ステータス|status|"
    r"どうなって|今日|きょう|さっき|直近の状態", re.IGNORECASE)
# 過去の一点を指している問い。ここでは鮮度で押し下げない(当時の記録こそ正解)。
_CAL_MD = r"(?<![\d/])(?:1[0-2]|0?[1-9])\s*[/月]\s*(?:3[01]|[12]\d|0?[1-9])(?![\d/])"
_CAL_YMD = r"\d{4}[/-](?:1[0-2]|0?[1-9])[/-](?:3[01]|[12]\d|0?[1-9])"
KAKO_RE = re.compile(
    _CAL_MD + r"|" + _CAL_YMD + r"|"
    r"昨日|一昨日|先週|先月|去年|当時|あの時|あのとき|以前|かつて|過去|"
    r"だった時|してた時|した時", re.IGNORECASE)
# 日付の存在と、問いの時点の指定を分ける。資料名や背景の日付だけで
# 明示的な現在問を過去問へ変えない。過去の時点が結び付く時は従来通り過去。
KAKO_SCOPE_RE = re.compile(
    r"昨日|一昨日|先週|先月|去年|当時|あの時|あのとき|以前|かつて|過去|"
    r"だった時|してた時|した時|"
    r"(?:" + _CAL_MD + r"|" + _CAL_YMD + r")\s*(?:の|は|に|現在)",
    re.IGNORECASE)
KAKO_WORD_RE = re.compile(
    r"昨日|一昨日|先週|先月|去年|当時|あの時|あのとき|以前|かつて|過去|"
    r"だった時|してた時|した時", re.IGNORECASE)
DATED_POINT_RE = re.compile(
    r"(?:" + _CAL_YMD + r"|" + _CAL_MD + r"(?:日)?)"
    r"(?:[ T]\d{1,2}:\d{2}(?::\d{2})?)?\s*"
    r"(?:(?:JST|UTC|Z|[+-]\d{2}:\d{2})\s*)?(?:の)?時点", re.IGNORECASE)

# 「今」としての有効期限(日)。None = 時間で失効しない。
#
# 出典 = 円卓の正本 events_*.jsonl 2024件を数えた実測(2026-08-29 Code)。
#   claim/progress … 終了(complete/blocked/answer)まで観測できた54件のうち
#                    **94.4%が1日以内**に終わっていた(最大11.8日)。
#   instruction    … 32件中 84.4%が7日以内。
#   question       … 41件中 78.0%が7日以内。
#   受信箱の状態通知 … 「手元に未処理がN件」38件の間隔は中央値4.78時間・p90 18.8時間。
#                    1日あれば必ず新しい物に置き換わっている。
#   complete/blocked/answer/note/proposal … 出来事の記録。起きた事は古くならない。
#   procedural(掟・手順) / semantic(解決台帳) / proof=王合格 … 時間で失効しない。
IMA_KIGEN_NICHI = {"claim": 1, "progress": 1, "instruction": 7, "question": 7}
JOUTAI_TSUUCHI = re.compile(r"手元に未処理が|の手元に未処理")   # 受信箱の状態通知(型はnote)
SHIJISHO_RE = re.compile(r"(^|/)instructions/|指示書")          # 指示書ファイル(=指示の別の形)
JOUTAI_TSUUCHI_KIGEN = 1


def jikan_ito(query: str) -> str:
    """問いが向いている時間。"ima"(今) / "kako"(過去の一点) / ""(時間を問うていない)。

    過去の時点を指定したら過去を採る。現在問に付いた日付ラベルは
    時点指定とは限らない。文体指定だけの文を時間指定で上書きしない。
    """
    full_text = str(query or "").strip()
    # 「今の時点」は現在。「時点」だけを過去とせず、日付に結び付く
    # 時点指定か、明示的な過去語がある時だけ過去の契約を優先する。
    if KAKO_WORD_RE.search(full_text) or DATED_POINT_RE.search(full_text):
        return "kako"
    sentences = [part.strip() for part in re.split(r"[。！？!?]", full_text) if part.strip()]
    intent = ""
    for text in sentences:
        kako = bool(KAKO_RE.search(text))
        ima = bool(IMA_RE.search(text))
        if kako and (not ima or KAKO_SCOPE_RE.search(text)):
            intent = "kako"
        elif ima:
            intent = "ima"
        # 時間の向きを持たない後続文では、取得済みの意図を消さない。
    return intent


def ima_kigen_nichi(item: dict) -> int | None:
    """この記録が「今」を答えられる日数。None = 失効しない。"""
    if str(item.get("proof") or "") == "王合格":
        return None                      # 王の裁定は時間で失効しない
    path = str(item.get("path") or "")
    if SHIJISHO_RE.search(path):
        # 指示書は「指示」の別の形。file になっているだけで、中身は割り当てられた仕事。
        # 2026-08-29 実測: 49日前の指示書_20260710 が「今なにしてる？」の3位に立っていた。
        return IMA_KIGEN_NICHI["instruction"]
    if str(item.get("memory_type") or "") in ("procedural", "semantic", "media"):
        return None                      # 手順・掟・解決台帳・素材
    honbun = str(item.get("text") or item.get("content")
                 or ((item.get("summary") or "") + " " + (item.get("snippet") or "")))
    if JOUTAI_TSUUCHI.search(honbun[:400]):
        return JOUTAI_TSUUCHI_KIGEN
    return IMA_KIGEN_NICHI.get(str(item.get("type") or ""))


def keika_nissuu(item: dict, now: datetime | None = None) -> float | None:
    """記録が書かれてから何日経ったか。時刻が読めなければ None。"""
    try:
        when = datetime.fromisoformat(str(item.get("timestamp") or ""))
    except (ValueError, TypeError):
        return None
    ima = now or datetime.now(when.tzinfo)
    if ima.tzinfo is None or when.tzinfo is None:
        ima = ima.replace(tzinfo=None)
        when = when.replace(tzinfo=None)
    return (ima - when).total_seconds() / 86400.0


def kigen_gire(item: dict, now: datetime | None = None) -> bool:
    """「今」の問いに対して、この記録はもう有効期限を過ぎているか。

    検索結果の行(shiage 済み)は本文を持たない(切れ端300字だけ)ので、期限は
    **本文が有る所(shiage)で焼いて ima_kigen に持たせる**。ここでは有ればそれを使う。
    2026-08-29 実測: これが無いと「手元に未処理がN件」の状態通知を行から見分けられず、
    20日前の在庫数が「今の状況を教えて」の1位に立ったままだった。
    """
    yaita = str(item.get("ima_kigen") or "")
    if yaita:
        try:
            kigiru = datetime.fromisoformat(yaita)
        except ValueError:
            return False
        ima = now or datetime.now(kigiru.tzinfo)
        if ima.tzinfo is None or kigiru.tzinfo is None:
            ima, kigiru = ima.replace(tzinfo=None), kigiru.replace(tzinfo=None)
        return ima > kigiru
    kigen = ima_kigen_nichi(item)
    if kigen is None:
        return False
    keika = keika_nissuu(item, now)
    return keika is not None and keika > kigen


def _window_active(valid_from: str, valid_until: str = "",
                   now: datetime | None = None) -> bool:
    """ISO時刻の訂正窓が今有効か。aware/naiveの混在は同じ規則へ揃える。"""
    start = _iso_datetime(valid_from)
    end = _iso_datetime(valid_until) if valid_until else None
    if start is None:
        return False
    current = now or datetime.now(start.tzinfo)
    if current.tzinfo is None or start.tzinfo is None:
        current, start = current.replace(tzinfo=None), start.replace(tzinfo=None)
        if end is not None:
            end = end.replace(tzinfo=None)
    elif end is not None and end.tzinfo is None:
        end = end.replace(tzinfo=start.tzinfo)
    return current >= start and (end is None or current < end)


def correction_current(item: dict, now: datetime | None = None) -> bool:
    """この行自身が、現在有効な訂正判断か。"""
    correction_id = str(item.get("correction_id") or "")
    if not correction_id:
        return False
    return _window_active(
        str(item.get("correction_valid_from") or ""),
        str(item.get("correction_valid_until") or ""),
        now,
    )


def correction_matches(item: dict, query: str) -> bool:
    """訂正の適用範囲がこの問いに当たるか。語を省略した台帳は全域訂正。"""
    terms = item.get("correction_match_terms") or item.get("match_terms") or []
    if not terms:
        return True
    question = normalize(query)
    return any(normalize(str(term)) in question for term in terms)


def kigen_iso(item: dict) -> str:
    """この記録が「今」を答えられなくなる時刻。失効しない物は空。"""
    kigen = ima_kigen_nichi(item)
    if kigen is None:
        return ""
    try:
        when = datetime.fromisoformat(str(item.get("timestamp") or ""))
    except (ValueError, TypeError):
        return ""
    return (when + timedelta(days=kigen)).isoformat(timespec="seconds")


# item_key は scripts/brain_text_utils.py へ分離済み

# ★政策(訂正・同族・主題)が**読む側**の欄。2026-09-21 ネオ ai-6162 が同じ穴の4度目で作った。
#   上の SEISAKU_NO_RAN は政策が**書いた**欄を起動束まで運ぶための一覧。
#   こちらは政策が**読む**欄を検索の結果まで運ぶための一覧。**2つとも足さないと黙って消える。**
#   実際に踏んだ: `moto_ga_teisei_sareta`(根拠が訂正された紙の印)を政策側に足したが
#   `shiage` の keep に足し忘れ、**snapshot には在るのに検索の結果から消えていた**
#   (本番で叩いて初めて分かった。空回しでは12件付いていた)。
#   同じ穴の記録: 1度目=2026-09-07 訂正の印 / 2度目=2026-09-09 主題の印 /
#   3度目=2026-09-12 起動束の keep / **4度目=2026-09-21 検索の keep**。
SEISAKU_NO_IRIGUCHI_RAN = (
    "correction_id", "correction_valid_from", "correction_valid_until",
    "correction_recorded_at", "correction_event_timestamp",
    "correction_scope", "correction_match_terms", "corrects",
    "supersession_windows", "kin_windows", "claim_key",
    "moto_ga_teisei_sareta",          # 根拠に挙げた記録が訂正された紙(提案書・報告書・話題)
    "supersedes_record_id", "superseded_record_ids",
)


_SHIAGE_KEEP = ("event_id", "question_event_id", "answer_event_id", "path", "report_path",
                "id", "title",
                "record_id", "memory_type", "trust", "proof", "type", "timestamp",
                "sender", "to", "topic", "source", "attachments", "media_id", "kind",
                "mime_type", "duration_seconds", "width", "height", "role", "tags",
                ) + SEISAKU_NO_IRIGUCHI_RAN


def _attach_rerank_fields(result: dict, item: dict, flat_text: str | None = None) -> None:
    """リランカー専用作業欄(_rerank_bun/_zenbun_yo/_text_zenbun)を付ける。"""
    if "_rerank_bun" in result:
        return
    flat = flat_text if flat_text is not None else " ".join(str(item.get("text") or "").split())
    atama = " / ".join(x for x in (str(item.get("topic") or ""), str(item.get("title") or "")) if x)
    naka = flat[:600]
    result["_rerank_bun"] = " ".join((atama + " " + result["summary"] + " " + naka).split())[:900]
    if dan2_ari():
        # 2段目(2026-09-14 ネオ ai-0a19)へ渡す全文版。**1段目(上の600/900)は1文字も変えない。**
        # 応答には出さない(_rerank_bun_hagasu が剥がす)。
        naka2 = flat[:dan2_honbun()]
        result["_zenbun_yo"] = " ".join(
            (atama + " " + result["summary"] + " " + naka2).split())[:dan2_bunmax()]
    if maxp_mado_ka() or dan2_pure_maxp_ari() or dan3_listwise_ari() or a3_template_latest_ari() or a3_wadai_mirror_ari():
        # MaxP / A-1(3窓純本文MaxP・接戦Listwise) / A-3(同型テンプレート・話題ミラー) のため
        # 全文作業欄 _text_zenbun を保持する。応答には絶対に出さない(_rerank_bun_hagasu が必ず削除)。
        result["_text_zenbun"] = flat[:12000]


_SHIAGE_BASE_CACHE: dict = {}


def _shiage_copy_val(v):
    t = type(v)
    if t is str or t is int or t is float or t is bool or v is None:
        return v
    if t is list:
        return [_shiage_copy_val(x) for x in v]
    if t is dict:
        return {k: _shiage_copy_val(val) for k, val in v.items()}
    return copy.deepcopy(v)


def shiage(item: dict, score: int, cache_bucket: dict | None = None, *, with_rerank_fields: bool = True) -> dict:
    """検索結果1件の形。全文(最大6000字)は返さず、要点1行+切れ端300字+出典で足りる。
    深掘りしたい時は event_id で正本(events/*.jsonl)か話題mdを開く。"""
    if cache_bucket is not None:
        ckey = (id(item), bool(with_rerank_fields))
        base = cache_bucket.get(ckey)
        if base is None:
            base = {key: item[key] for key in _SHIAGE_KEEP if item.get(key)}
            base["summary"] = _safe_summary(str(item.get("summary") or item.get("title") or ""), 160)
            if with_rerank_fields:
                flat = " ".join(str(item.get("text") or "").split())
                base["snippet"] = flat[:300]
                _attach_rerank_fields(base, item, flat)
            else:
                raw_text = str(item.get("text") or "")
                base["snippet"] = " ".join(raw_text[:1200].split())[:300] if len(raw_text) > 1200 else " ".join(raw_text.split())[:300]
            kigen = kigen_iso(item)
            if kigen:
                base["ima_kigen"] = kigen
            cache_bucket[ckey] = base
        result = {k: _shiage_copy_val(v) for k, v in base.items()}
        result["score"] = round(float(score), 2)
        if not with_rerank_fields:
            result["_item_ref"] = item
        return result
    result = {key: item[key] for key in _SHIAGE_KEEP if item.get(key)}
    result["score"] = round(float(score), 2)
    result["summary"] = _safe_summary(str(item.get("summary") or item.get("title") or ""), 160)
    if with_rerank_fields:
        flat = " ".join(str(item.get("text") or "").split())
        result["snippet"] = flat[:300]
        _attach_rerank_fields(result, item, flat)
    else:
        raw_text = str(item.get("text") or "")
        result["snippet"] = " ".join(raw_text[:1200].split())[:300] if len(raw_text) > 1200 else " ".join(raw_text.split())[:300]
        result["_item_ref"] = item
    # 「今」としての有効期限(2026-08-29)。失効しない物には付けない(応答を太らせない)。
    kigen = kigen_iso(item)
    if kigen:
        result["ima_kigen"] = kigen
    return result


def lazy_shiage_ari() -> bool:
    """Stage-1 候補の遅延 shiage() 有効旗（既定 ON、KINGDOM_LAZY_SHIAGE=0 で即時従来経路へ復帰）。"""
    return os.environ.get("KINGDOM_LAZY_SHIAGE", "1").strip().lower() not in ("0", "false", "no", "off")


HAY_CACHE: dict[str, tuple[list[str], float]] = {}
HAY_FIELDS = ("text", "summary", "title", "topic", "sender", "to", "path",
              "event_id", "thread_id", "type", "proof")


SOUTEI_CACHE: dict = {}


def soutei_yomu(root) -> tuple[str, dict]:
    """想定質問の索引(doc2query)を読む。無ければ空を返す(脳は止めない)。

    なぜ要るか(実測 2026-09-01・golden_v2 276問・凍結土俵base-20260828):
      A型(固有名詞なし・王の言い方)は候補天井0.742で、26%が**候補にすら入らない**。
      落ちた17問は全て「うちの絵を作るやつ」の類で、記録側は技術用語で書かれている。
      **語彙が接触していない**のが正体で、直し方は「当たる語を記録側に増やす」(doc2query)。
    焼き方 = scripts/soutei_shitsumon.py。記録の正本は1バイトも触らない別索引。
    """
    if root is None:
        return "", {}
    try:
        path = Path(root) / "roundtable" / "brain" / "soutei.json"
        st = path.stat()
    except (OSError, TypeError, ValueError):
        return "", {}
    # ⚠️ 覚え書きの鍵には**どのファイルか**まで入れる(2026-09-09 ネオ・同じ型を今日3件見た)。
    #    凍結土俵は本番から時刻を保つ形で写せるので、時刻と大きさだけの鍵では
    #    本番と土俵を取り違える。_SHUDAI_OBOE で実際に起きていた穴と同じ形。
    shimon = "%s|%d|%d" % (path, st.st_mtime_ns, st.st_size)
    hit = SOUTEI_CACHE.get(shimon)
    if hit is not None:
        return shimon, hit
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        rows = {key: " ".join(value.get("toi") or [])
                for key, value in (data.get("rows") or {}).items()}
    except (OSError, ValueError, AttributeError):
        rows = {}
    SOUTEI_CACHE.clear()
    SOUTEI_CACHE[shimon] = rows
    return shimon, rows


_HAY_ROWS: dict[tuple[str, tuple[str, ...]], str] = {}
_HAY_VIEW_ROWS: dict[tuple[str, tuple[str, ...]], str] = {}


def haystacks_of(snapshot: dict, items: list[dict], root=None) -> tuple[list[str], float]:
    """版内の結果と、版をまたいで内容が同一の正規化済み検索文を再利用する。"""
    global _HAY_ROWS, _HAY_VIEW_ROWS
    version = str(snapshot.get("version") or "")
    shimon, soutei = soutei_yomu(root)
    snap_key = _SNAPSHOT_ID_KAGI.get(id(snapshot))
    fast = (snap_key is not None and _snapshot_cache_on()
            and _SNAP_ITEMS_CACHE.get((id(snapshot), snap_key)) is items)
    if fast:
        ban = f"fast:{snap_key}|v:{version}|soutei:{shimon}|n:{len(items)}"
        hit = HAY_CACHE.get(ban)
        if hit is not None:
            return hit

    rows = []
    for item in items:
        key = item_key(item)
        fields = tuple(str(item.get(field) or "") for field in HAY_FIELDS)
        rows.append((key, fields + (soutei.get(key, ""),)))
    texts = None
    if not fast:
        # 加工済みの部分集合などには、順序も含めた従来の版内キーを使う。
        texts = [" ".join(fields) for _, fields in rows]
        fingerprint = hashlib.sha256()
        for text in texts:
            raw = text.encode("utf-8")
            fingerprint.update(len(raw).to_bytes(8, "big"))
            fingerprint.update(raw)
        ban = version + "|soutei:" + shimon + "|content:" + fingerprint.hexdigest()
        hit = HAY_CACHE.get(ban)
        if hit is not None:
            return hit

    previous = _HAY_ROWS
    previous_view = _HAY_VIEW_ROWS
    current = {}
    hays = []
    for index, row in enumerate(rows):
        # 同じIDが複数あっても、全文字列フィールドが一致する行だけ使う。
        hay = previous.get(row)
        if hay is None:
            hay = previous_view.get(row)
        if hay is None:
            hay = normalize(" ".join(row[1]) if texts is None else texts[index])
        current[row] = hay
        hays.append(hay)
    avgdl = (sum(len(h) for h in hays) / len(hays)) if hays else 1.0
    global _HAY_PREV_BM25
    with _HAY_LOCK:
        if fast:
            _HAY_ROWS = current
            _HAY_VIEW_ROWS = {}
            # 版更新直前に旧版の BM25 raw_hits を1世代分だけ退避し、次版へ増分引き継ぎする
            for _old_hays, _ in HAY_CACHE.values():
                _old_entry = _HAY_INDEX_OBOE.get(id(_old_hays))
                if _old_entry and _old_entry.get("term_raw_hits"):
                    _HAY_PREV_BM25 = (_old_hays, _old_entry)
                    break
        else:
            # as_of などの部分集合で無加工snapshotのキャッシュを追い出さない。
            _HAY_VIEW_ROWS = current
        HAY_CACHE.clear()
        _HAY_INDEX_OBOE.clear()
        _SHIAGE_BASE_CACHE.clear()
        HAY_CACHE[ban] = (hays, avgdl)
    return hays, avgdl


_HAY_LOCK = threading.RLock()
_HAY_INDEX_OBOE: dict[int, dict] = {}
_HAY_PREV_BM25: tuple[list[str], dict] | None = None


def _hay_index_for(hays: list[str], avgdl: float, items: list[dict] | None = None) -> dict | None:
    """HAY_CACHE の現行版に紐づくBM25転置キャッシュ＆静的メタ(順位は1ビットも変えない)。"""
    global _HAY_PREV_BM25
    with _HAY_LOCK:
        if not any(v[0] is hays for v in HAY_CACHE.values()):
            return None
        hid = id(hays)
        entry = _HAY_INDEX_OBOE.get(hid)
        if entry is None:
            norms = _build_hay_norms(hays, avgdl)
            prev_carry = None
            if _HAY_PREV_BM25 is not None:
                _prev_hays, _prev_entry = _HAY_PREV_BM25
                prev_carry = _build_incremental_bm25_carry(_prev_hays, _prev_entry, hays)
                # 増分対応表を作ったら旧hays参照を即解放して2世代分を抱え込まない
                _HAY_PREV_BM25 = None
            entry = {
                "norms": norms,
                "term_postings": {},
                "term_raw_hits": {},
                "prev_carry": prev_carry,
                "items_id": None,
                "item_keys": None,
                "by_key": None,
                "static_omake": None,
                "parsed_ts": None,
                "has_corr": None,
                "shiage_bucket": _SHIAGE_BASE_CACHE.setdefault(hid, {}),
            }
            _HAY_INDEX_OBOE[hid] = entry
    if items is not None and (entry.get("items_ref") is not items) and len(items) == len(hays):
        keys = [item_key(it) for it in items]
        by_key = {k: it for k, it in zip(keys, items)}
        static_omake = []
        parsed_ts = []
        has_corr = []
        for it in items:
            om = 0.0
            trust = it.get("trust")
            if trust in {"verified_record", "verified_report", "resolved"}:
                om += 0.8
            elif trust == "measured_claim":
                om += 0.4
            if str(it.get("proof") or "") == "王合格":
                om += 1.0
            if it.get("digest"):
                om += 0.5
            static_omake.append(om)
            ts = str(it.get("timestamp") or "")
            try:
                when = datetime.fromisoformat(ts)
            except (ValueError, TypeError):
                when = None
            parsed_ts.append((ts, when))
            has_corr.append(bool(correction_current(it)))
        entry["items_ref"] = items
        entry["items_id"] = id(items)
        entry["item_keys"] = keys
        entry["by_key"] = by_key
        entry["static_omake"] = static_omake
        entry["parsed_ts"] = parsed_ts
        entry["has_corr"] = has_corr
        entry["shiage_bucket"].clear()
        entry.pop("by_eid_text", None)
    return entry



RERANK_URL = os.environ.get("KINGDOM_RERANK", "http://127.0.0.1:8794/v1/rerank")
RERANK_KOUHO = 50        # **席の資格**(王/道具/主題/観点の席に座れるのは上位50件まで)。ここは動かさない。
#   ⚠️旧注記「天井(R@50)=0.85」は腐っていた。2026-09-14実測の天井は 0.925(akai_uchiwake.py)。
DAN1_KOUHO_KITEI = 50    # 1段目の並べ替え役へ渡す候補の数(蓋)。環境 KINGDOM_RERANK_KOUHO で変えられる。
#   ★なぜ分けたか(実測 2026-09-14 ネオ ai-8742): 「50位内に拾えていない」18本を蓋の手前で測ったら、
#     **18本中17本は候補の中に居た**(1段目の順位で 57〜1051位)。候補総数は問いごとに 13〜1,567件。
#     つまり拾えていないのではなく、**50件の蓋がリランカーに見せていなかった**。
#     蓋を100にすると4本・200で7本・400で10本・800で16本が並べ替え役へ届く(届く=1位になる、ではない)。
#     席の資格(RERANK_KOUHO)は50のまま分けてある ―― 蓋を広げても席は広がらない(教訓9)。
RERANK_TIMEOUT = 20

# ---- 2段目(上位だけ全文で付け直す)の旗と寸法(2026-09-14 ネオ ai-0a19) ----------
# なぜ要るか(実測 2026-09-14・同じ276問・凍結土俵base-20260828・対の検定):
#   1段目が並べ替え役へ渡す文は「話題+題名+要点+本文600字」を900字で切った物で、
#   **答えが600字より後ろに在る記録は見えていなかった**。上位25件だけ本文3000字で
#   付け直すと R@1 0.542→0.633(+0.091)。良くなった22本・**悪くなった0本**・
#   McNemar p≈7.6e-06。速さは0.77→1.45秒/問(1.88倍)。
#   全件を全文にすると +0.112 まで伸びるが**4本が2位へ落ちる**(悪化ゼロは10件と25件だけ)。
#   世界の型 = cascade / telescoping ranking(安い段で絞り、高い段は上位だけに掛ける)。
#   ⚠️ 2026-08-29の注記「本文600字が最良(800=0.517,1000=0.467)」は n=85 の信頼区間
#      ±0.105 の中の揺れだった(実データ3,771件では800字と1000字はほぼ同じ入力)。
#   ⚠️ 並べ替え役の1024トークンの蓋は王国が自分で付けた物(~/reranker/server.py:26)。
#      モデル ruri-v3-reranker-310m は config.json で max_position_embeddings 8192。
#      2段目を効かせるには reranker.service に RURI_MAX_LEN=8192 が要る
#      (drop-in ~/.config/systemd/user/reranker.service.d/10-maxlen.conf・2026-09-14 投入)。
#      1段目は「最長の問い50字+文900字=950字」でトークンは必ず958以下 ＝ **1024には数学的に当たらない**
#      ので、蓋を上げても1段目は1文字も変わらない(上げる前と後で同じ基線を測って実証済み)。
DAN2_FUKASA_KITEI = 50   # 付け直す件数＝候補50件ぜんぶ。
#   ★2026-09-14 17:28 ネオ ai-3d3e が 25→50 へ上げた（同じ276問・凍結土俵base-20260828・対の比較）:
#     深さ25  R@1 0.633 / R@3 0.800 / MRR@10 0.716 / 無い物正答率 0.889
#     深さ50  R@1 0.658 / R@3 0.833 / MRR@10 0.748 / 無い物正答率 0.917  ← **4指標とも改善**
#     McNemar正確: **1位に入った6問 / 落ちた0問 → p=0.03125（有意）**
#     上がった6問は全部 **26〜47位** から1位＝深さ25では2段目が届いていなかった範囲。理屈と一致。
#     代償は速さだけ: 検索 574秒 → 645秒/276問（2.08 → 2.34秒/問・1.12倍）
#   ⚠️以前の注記「全件=100%だが4本悪化」は **深さ25を基準にした時の内訳**で、
#     純増では負けていない（今回の実測で落ちた問いは0本）。切り戻しは KINGDOM_RERANK_DAN2_FUKASA=25 の一手。
DAN2_HONBUN = 3000       # 2段目へ渡す本文の長さ。環境 KINGDOM_DAN2_HONBUN で変えられる
DAN2_BUNMAX = 3200       # 頭(話題/題名/要点)込みの上限。環境 KINGDOM_DAN2_BUNMAX で変えられる
DAN2_MOTO_OMOMI = 0.375  # 2段目の位置項のうち「初期融合(BM25+意味+加点)の順位」に掛ける重み
DAN2_D1_OMOMI = 0.375    # 2段目の位置項のうち「1段目の順位」に掛ける重み
#   ★2026-11-15 ギア(Gear): 旧式は2段目でも 0.75/(1段目の順位+1) を足していた。1段目は本文600字しか
#     見ていないので、その順位を2段目(本文3000字)の位置項に使うと、600字で損をした記録が
#     3000字で取り返しても位置項で負け続ける(同じ誤りを二度数える)。初期融合順を独立の事前分布として使う。
#     重み 0.75/0.50/0.35 を比べ 0.50 が最良(work/gear_search_r1_20261115/diag_true_moto.py)。
#   ★2026-09-20 ネオ ai-bed1: **既定値は1ミリも変えていない。外から測れるようにしただけ。**
#     なぜ要ったか: 1段目の長さには実測の根拠がコードに在る（600字が最良・800=0.517・1000=0.467）のに、
#     **2段目の 3000/3200 には根拠が無く、しかも環境変数が無いので誰も測れなかった**。
#     他の6つ(KINGDOM_RERANK_KOUHO / _DAN2 / _DAN2_FUKASA / _MAXP / KINGDOM_IMI_KOUHO / KINGDOM_MOJI_TSUYOSUGI)
#     と同じ形に揃えただけで、新しい仕組みではない。
#     ⚠️並べ替え役は ruri-v3-reranker-310m で **max_len=8192**（実測 /health）。
#       世界で言われる「512を超えると黙って捨てられる」機種ではないので、3000は切り捨てられていない。
#       ただし1段目の実測どおり「長いほど無関係な尾が点を薄める」性質は在るので、最適値は実測で決める。


def dan2_honbun() -> int:
    """2段目へ渡す本文の長さ。呼ぶたび環境を読む(壊れた値は既定へ落ちる・脳は止めない)。"""
    try:
        n = int(os.environ.get("KINGDOM_DAN2_HONBUN", str(DAN2_HONBUN)))
        return n if n > 0 else DAN2_HONBUN
    except (TypeError, ValueError):
        return DAN2_HONBUN


def dan2_bunmax() -> int:
    """2段目へ渡す全体の上限。呼ぶたび環境を読む(壊れた値は既定へ落ちる)。"""
    try:
        n = int(os.environ.get("KINGDOM_DAN2_BUNMAX", str(DAN2_BUNMAX)))
        return n if n > 0 else DAN2_BUNMAX
    except (TypeError, ValueError):
        return DAN2_BUNMAX


def dan1_kouho() -> int:
    """1段目の並べ替え役へ渡す候補の数。呼ぶたび環境を読む(切り戻しは環境を消すだけ)。
    壊れた値が来ても既定へ落ちる(脳は止めない・dan2_fukasa と同じ作法)。"""
    try:
        n = int(os.environ.get("KINGDOM_RERANK_KOUHO", str(DAN1_KOUHO_KITEI)))
    except (TypeError, ValueError):
        return DAN1_KOUHO_KITEI
    return max(1, n)


def dan2_ari() -> bool:
    """2段目を使うか。呼ぶたび環境を読む(切り戻しは KINGDOM_RERANK_DAN2=0 の一手で済む)。"""
    return os.environ.get("KINGDOM_RERANK_DAN2", "1") != "0"


def dan2_fukasa() -> int:
    """付け直す件数。壊れた値が来ても既定へ落ちる(脳は止めない)。"""
    try:
        fukasa = int(os.environ.get("KINGDOM_RERANK_DAN2_FUKASA", str(DAN2_FUKASA_KITEI)))
    except (TypeError, ValueError):
        return DAN2_FUKASA_KITEI
    return max(0, fukasa)


DAN2_DYNAMIC_KMIN_KITEI = 20        # 2段目動的深さの最低保証件数(Stage-2.5 Pure MaxP Top-20と整合)
DAN2_DYNAMIC_YUGO_GAP_KITEI = 10.0  # 1段目top1とのrr_yugo差上限(凍結最大7.21に対し+2.79余裕)


def dan2_dynamic_depth_ari() -> bool:
    """2段目のスコア差ベース動的深さ枝刈り(A-2a)を有効にするか。KINGDOM_DAN2_DYNAMIC_DEPTH=0 で即無効化。"""
    v = os.environ.get("KINGDOM_DAN2_DYNAMIC_DEPTH", "1").strip().lower()
    return v not in ("0", "false", "off", "no")


def dan2_dynamic_kmin() -> int:
    try:
        v = int(os.environ.get("KINGDOM_DAN2_DYNAMIC_KMIN", str(DAN2_DYNAMIC_KMIN_KITEI)))
    except (TypeError, ValueError):
        return DAN2_DYNAMIC_KMIN_KITEI
    return max(0, v)


def dan2_dynamic_yugo_gap() -> float:
    try:
        v = float(os.environ.get("KINGDOM_DAN2_DYNAMIC_YUGO_GAP", str(DAN2_DYNAMIC_YUGO_GAP_KITEI)))
    except (TypeError, ValueError):
        return DAN2_DYNAMIC_YUGO_GAP_KITEI
    if not math.isfinite(v) or v < 0.0:
        return DAN2_DYNAMIC_YUGO_GAP_KITEI
    return v


def _dan2_dynamic_fukasa(naranda: list[dict], max_fukasa: int) -> int:
    """1段目 rr_yugo の top1 との差で2段目採点件数を動的決定する(A-2a: yugo_km20_d10)。
    スコア欠損・非有限値があれば fail-open で max_fukasa まで全件評価する。"""
    limit = min(max_fukasa, len(naranda))
    if limit <= 0 or not dan2_dynamic_depth_ari():
        return limit
    k_min = min(dan2_dynamic_kmin(), limit)
    if limit <= k_min:
        return limit
    top1_raw = naranda[0].get("rr_yugo")
    if not isinstance(top1_raw, (int, float)) or isinstance(top1_raw, bool) or not math.isfinite(top1_raw):
        return limit
    top1 = float(top1_raw)
    gap_max = dan2_dynamic_yugo_gap()
    last_keep = k_min
    for i in range(k_min, limit):
        val = naranda[i].get("rr_yugo")
        if not isinstance(val, (int, float)) or isinstance(val, bool) or not math.isfinite(val):
            return limit
        if (top1 - float(val)) <= gap_max:
            last_keep = i + 1
    return min(last_keep, limit)


# 2026-09-01(バグ台帳:1516/:1533): リランカーの故障・脱落は「静かな劣化」なので必ず痕跡を残す。
# 直近の一回分だけ持つ(状態を太らせない)。status_payload と検索結果の両方から見える。
RERANK_JOUTAI: dict = {"state": "未実行", "at": "", "why": "", "datsuraku": 0}
# 並べ替え役が落ちた累計。★RERANK_JOUTAI の外に置く ―― 2段目が RERANK_JOUTAI.clear() するので
#   中に入れると2段目の失敗が数えられない(2026-09-14 ネオ ai-8742)。
RERANK_SHIPPAI = {"kaisu": 0, "saigo": ""}
RERANK_SHIPPAI_JOUTAI = {"fail_open", "tensuu_nashi", "zenmetsu"}   # これが出たら順位は本来より悪い


def _rerank_joutai_kaku(state: str, why: str = "", datsuraku: int = 0) -> None:
    """並べ替え役の状態を**1箇所で**書く（出口ごとに書くと片方が古くなる）。

    2026-09-05 修理(バグ探し低#5): 従来は「実際に並べ替えた時」しか書いていなかった。
    候補1件以下(not_needed)や正本イベント直引き(not_required)の経路は診断だけを
    動かし status 側を据え置いたので、そういう問いだけを何度浴びても
    brain_status の rerank は『未実行』のまま張り付いた。読む側からは
    「まだ一度も呼ばれていない」のか「呼ばれたが要らなかった」のか区別できない。
    要らなかった事も1つの結果として書く。欄の意味(=最後に呼ばれた時の記録)を揃える。
    """
    RERANK_JOUTAI.update({
        "state": state,
        "at": datetime.now().astimezone().isoformat(timespec="seconds"),
        "why": why,
        "datsuraku": datsuraku,
    })
    if state in RERANK_SHIPPAI_JOUTAI:
        # 落ちた事は「最後の状態」だけでなく**回数**でも残す。
        # 1回落ちただけで測定の数字は本番より悪く出るので、測る台がここを見て捨てられるようにする。
        RERANK_SHIPPAI["kaisu"] += 1
        RERANK_SHIPPAI["saigo"] = "%s: %s" % (state, why[:120])


def maxp_shurui() -> str:
    """MaxPの旗(2026-09-05 ネオ・くくる優先5)。0=今まで(先頭600字) / 1=語の重なりで区間を選ぶ(1回目・
    凍結85問でR@1 0.500→0.433と悪化=競合の無関係な長い記録が癖のある2文字で押し上がった) /
    2=本物のMaxP(区間ごとに並べ替え役に点を付けさせ、記録の点は最大)。呼ぶたびに環境を読む。"""
    return os.environ.get("KINGDOM_RERANK_MAXP", "0")


def maxp_mado_ka() -> bool:
    return maxp_shurui() in ("1", "2")


MAXP_MADO_SAIDAI = 8     # 1記録あたり並べ替え役へ出す追加区間の上限(50候補×8=400対まで・48件ずつ処理で≈9回)


def mado_tachi(query: str, text: str, haba: int = 600, hosa: int = 300, saidai: int = MAXP_MADO_SAIDAI) -> list[str]:
    """先頭以外の区間を並べる(本物のMaxP用)。数が上限を超える時だけ、問いの語の重なりで上位を選ぶ
    (費用の蓋であって点ではない。点は並べ替え役が付ける)。同点は先頭寄り。"""
    text = " ".join(str(text or "").split())
    if len(text) <= haba:
        return []
    starts = list(range(hosa, len(text) - haba, hosa)) + [len(text) - haba]
    starts = sorted(set(s for s in starts if s > 0))
    if len(starts) <= saidai:
        return [text[s:s + haba] for s in starts]
    terms = [t for t in query_terms(query) if len(t) >= 2]
    tenpyo = []
    for s in starts:
        mado = text[s:s + haba]
        nm = normalize(mado)
        tenpyo.append((-sum(1 for t in terms if t in nm), s, mado))
    tenpyo.sort()
    return [m for _, _, m in tenpyo[:saidai]]


MAXP_MADO_HABA = 600     # 8/29実測: 本文600字が最良(800=0.517, 1000=0.467)。長さは変えず、切る位置だけ変える
MAXP_MADO_HOSA = 300     # 半分ずつずらす(Dai&Callan 2019 の 150語/75語 と同じ比)


def saiyo_mado(query: str, text: str, haba: int = MAXP_MADO_HABA, hosa: int = MAXP_MADO_HOSA,
               _terms_pre: list[str] | None = None) -> str:
    """問いの語が一番多く入る区間を返す(MaxP・Dai&Callan 2019)。
    点は「問いの語(query_terms)のうち区間に在る語の数」。同点なら先頭寄り(=今までの挙動へ倒れる)。
    問いの語が1つも無い時も先頭を返す=旗を立てても最悪で今と同じ。"""
    return _speed_cache.saiyo_mado_fast(
        query, text, haba, hosa, normalize, query_terms, terms_pre=_terms_pre,
    )




# 2026-11-21 進化コア(ルートA-1/A-3): 独立モジュール brain_evolve_rank へ分離
import brain_evolve_rank as _evolve_rank
from brain_evolve_rank import (
    DAN2_PURE_MAXP_MIN_LEN,
    DAN2_PURE_MAXP_STRIDE,
    DAN2_PURE_MAXP_TOPK,
    DAN2_PURE_MAXP_WEIGHT,
    DAN2_PURE_MAXP_WIN,
    DAN3_LISTWISE_BOOST_DEFAULT,
    DAN3_LISTWISE_MARGIN,
    DAN3_LISTWISE_MODEL_DEFAULT,
    DAN3_LISTWISE_TIMEOUT_DEFAULT,
    DAN3_LISTWISE_TOPK,
    _a3_template_latest_promote,
    _a3_wadai_mirror_resolve,
    _call_rerank_batches,
    _dan2_pure_maxp,
    _dan3_ask_listwise,
    _dan3_listwise_boost,
    _dan3_listwise_consensus,
    _dan3_listwise_model,
    _dan3_make_card,
    _dan3_ollama_url,
    _idf_best_window,
    _logit,
    _row_ts_key,
    _sem_best_window,
    _skeleton_similarity,
    _template_skeleton,
    a3_template_latest_ari,
    a3_wadai_mirror_ari,
    dan2_pure_maxp_ari,
    dan3_listwise_ari,
)

def narabe_naosu(query: str, rows: list[dict], limit: int,
                 diagnostics: dict | None = None,
                 dan2: bool = True,
                 best_cnum: dict[str, int] | None = None,
                 idx_cache: dict | None = None,
                 hays: list[str] | None = None,
                 total_docs: int = 0,
                 enable_evolve: bool = False) -> list[dict]:
    """交差符号化リランカー(ruri-v3-reranker-310m・3060)で並べ直す(2026-08-21 ネオ)。

    なぜ要るか(実測 2026-08-20): 候補50件の中に正解が入っているのは85%(天井)なのに、
    上位3件に並べられているのは51.7%。**0.33を並べ方だけで損していた**。
    埋め込みの類似度は「似ているか」しか見ないが、リランカーは質問と文書を突き合わせる。

    落ちていたら今までどおりの順で返す。脳は止めない。
    """
    if _is_cpu_agent_active():
        if diagnostics is not None:
            diagnostics["rerank"] = {"state": "disabled_cpu_agent", "why": "cpu_agent_mode"}
        return list(rows[:max(1, int(limit))]) if rows else []
    if not rows or len(rows) < 2:
        _rerank_joutai_kaku("not_needed", "candidate_count_lt_2")
        if diagnostics is not None:
            diagnostics["rerank"] = {
                "state": "not_needed", "why": "candidate_count_lt_2"}
        return rows[:limit]
    kouho = rows[:dan1_kouho()]
    dan2_chuu = bool(kouho and kouho[0].get("_dan2_chuu"))
    bunsho = []
    moto = []            # bunsho[k] がどの候補(kouho の添字)の文か。本物のMaxPでは1候補に複数の文が並ぶ
    ten: dict[int, float] = {}
    _maxp2 = (maxp_shurui() == "2")
    for bangou, r in enumerate(kouho):
        if dan2_chuu and "_dan2_reuse_raw" in r and not (_maxp2 and r.get("_text_zenbun")):
            ten[bangou] = float(r["_dan2_reuse_raw"])
            continue
        bun = str(r.get("_rerank_bun") or "")
        if maxp_shurui() == "1" and r.get("_text_zenbun"):
            # MaxP(2026-09-05): 先頭600字の代わりに問いに一番近い600字。頭(話題/題名/要点)は同じ。
            atama = " / ".join(x for x in (str(r.get("topic") or ""), str(r.get("title") or "")) if x)
            naka = saiyo_mado(query, str(r.get("_text_zenbun") or ""))
            bun = " ".join((atama + " " + str(r.get("summary") or "") + " " + naka).split())[:900]
        if not bun:      # 古い形の行(意味層の拾い物など)への保険
            bun = " ".join((str(r.get("summary") or "") + " " +
                            str(r.get("snippet") or "")).split())[:900]
        bunsho.append(bun)
        moto.append(bangou)
        if maxp_shurui() == "2" and r.get("_text_zenbun"):
            # 本物のMaxP(2026-09-05): 先頭600字(上のbun)に加え、後ろの区間も同じ頭を付けて並べ替え役へ。
            # 記録の点は区間の最大を取る(下の ten の組み立て)。
            atama = " / ".join(x for x in (str(r.get("topic") or ""), str(r.get("title") or "")) if x)
            for mado in mado_tachi(query, str(r.get("_text_zenbun") or "")):
                bunsho.append(" ".join((atama + " " + str(r.get("summary") or "") + " " + mado).split())[:900])
                moto.append(bangou)
    got = {"results": []}
    if bunsho:
        try:
            import urllib.request
            body = json.dumps({"query": query, "documents": bunsho}).encode("utf-8")
            _post_fn = getattr(_evolve_rank, "_http_post_json", None)
            if _post_fn is not None:
                got = _post_fn(RERANK_URL, body, RERANK_TIMEOUT)
            else:
                req = urllib.request.Request(RERANK_URL, data=body,
                                             headers={"Content-Type": "application/json"})
                with urllib.request.urlopen(req, timeout=RERANK_TIMEOUT) as res:
                    got = json.loads(res.read().decode("utf-8"))
        except Exception as exc:
            # 落ちても脳は止めないが、**落ちた事は残す**(旧版は無言でfail-openし劣化が観測不能だった)
            _rerank_joutai_kaku("fail_open", f"{type(exc).__name__}: {exc}")
            if diagnostics is not None:
                diagnostics["rerank"] = {
                    "state": "fail_open", "why": f"{type(exc).__name__}: {exc}"[:160]}
            return rows[:limit]                   # リランカーが居なくても脳は今までどおり動く
    # 融合(2026-08-29 Code・第1段): リランカーの点だけで並べ替えない。
    for item in got.get("results", []):
        index = int(item.get("index", -1))
        if 0 <= index < len(bunsho):
            k = moto[index]
            t = float(item.get("relevance_score") or 0)
            if t > ten.get(k, -1.0):        # 同じ候補に複数の文がある時は最大(MaxP)。1文なら今までと同じ
                ten[k] = t
    if not ten:
        _rerank_joutai_kaku("tensuu_nashi", "リランカーが1件も採点を返さなかった", len(kouho))
        if diagnostics is not None:
            diagnostics["rerank"] = {
                "state": "tensuu_nashi", "why": "no_scores"}
        return rows[:limit]
    out = []
    need_dan2 = bool(dan2 and dan2_ari())
    for index, row in enumerate(kouho):
        raw = ten.get(index)
        if raw is None:
            continue
        p = min(max(raw, 1e-9), 1 - 1e-9)
        if dan2_chuu:
            moto_i = int(row.get("_moto_idx", index))
            pos_term = DAN2_MOTO_OMOMI / (moto_i + 1) + DAN2_D1_OMOMI / (index + 1)
        else:
            moto_i = index
            pos_term = 0.75 / (index + 1)
        yugo = math.log(p / (1 - p)) + pos_term
        trust = row.get("trust")
        if trust in {"verified_record", "verified_report", "resolved"}:
            yugo += 0.5 * 0.8
        elif trust == "measured_claim":
            yugo += 0.5 * 0.4
        if str(row.get("proof") or "") == "王合格":
            yugo += 0.5 * 1.0
        yugo += 0.5 * float(row.get("imi") or 0)
        row = dict(row)
        if need_dan2:
            row["_moto_idx"] = moto_i
            row["_d1_raw"] = raw
        if dan2_chuu:
            row["_d2_raw"] = raw
            row["_d2_bonus"] = yugo - math.log(p / (1 - p))
        row["rerank"] = round(raw, 3)
        row["rr_yugo"] = round(yugo, 2)
        out.append(row)
    if not out:
        _rerank_joutai_kaku("zenmetsu", "採点のある候補が0件", len(kouho))
        if diagnostics is not None:
            diagnostics["rerank"] = {
                "state": "zenmetsu", "why": "all_candidates_dropped"}
        return rows[:limit]
    # 採点を返さなかった候補は黙って消える(旧版の :1533)。何件消えたかを残す。
    _rerank_joutai_kaku("ok", "", len(kouho) - len(out))
    if diagnostics is not None:
        diagnostics["rerank"] = {
            "state": "ok", "why": "", "datsuraku": len(kouho) - len(out)}
    if dan2_chuu:
        out.sort(key=lambda r: (-r["rr_yugo"], int(r.get("_moto_idx", 0))))
    else:
        out.sort(key=lambda r: -r["rr_yugo"])
    if need_dan2:
        # 2段目(2026-09-14): 上位だけ全文で付け直す。dan2=False で呼ばれた自分自身は入らない
        out = _dan2_tsukenaosu(query, out, diagnostics)
        if enable_evolve and len(out) >= 2:
            _pre_margin = float(out[0].get("rr_yugo") or 0.0) - float(out[1].get("rr_yugo") or 0.0)
            out = _dan2_pure_maxp(query, out, best_cnum=best_cnum,
                                  idx_cache=idx_cache, hays=hays, total_docs=total_docs)
            out = _dan3_listwise_consensus(query, out, idx_cache=idx_cache,
                                           hays=hays, total_docs=total_docs,
                                           pre_maxp_margin=_pre_margin)
        for r in out:
            r.pop("_moto_idx", None)
            r.pop("_d1_raw", None)
            r.pop("_dan2_reuse_raw", None)
            r.pop("_d2_raw", None)
            r.pop("_d2_bonus", None)
    return out[:limit]


def _dan2_tsukenaosu(query: str, naranda: list[dict],
                     diagnostics: dict | None = None) -> list[dict]:
    """2段目(2026-09-14 ネオ ai-0a19): 上位 DAN2_FUKASA 件だけ全文で付け直す。

    1段目は速さのために900字のまま残す(50件×900字)。そこで見えない
    「答えが本文600字より後ろに在る記録」を、上位だけ本文3000字で突き合わせ直す。
    実測(同じ276問・凍結土俵base-20260828): R@1 0.542→0.633・良く22本・**悪く0本**・
    McNemar p≈7.6e-06・0.77→1.45秒/問。数字の出所は DAN2_* の注記。

    守る事:
      ・**元の行は触らない**(写しの上でだけ文を差し替える)。応答に全文を残さないため
      ・2段目が落ちても1段目の順をそのまま返す(脳は止めない)
      ・1段目の観測(RERANK_JOUTAI)を消さない。2段目の結果は "dan2" の欄へ入れる
    """
    fukasa = dan2_fukasa()
    if fukasa <= 0 or len(naranda) < 2:
        return naranda
    eff_fukasa = _dan2_dynamic_fukasa(naranda, fukasa)
    ue, shita = naranda[:eff_fukasa], naranda[eff_fukasa:]
    if len(ue) < 2:
        return naranda
    moto_bun: dict[str, str | None] = {}
    ue2: list[dict] = []
    kaeta = 0
    for idx, row in enumerate(ue):
        moto_bun[item_key(row)] = row.get("_rerank_bun")
        zen = row.get("_zenbun_yo")
        utsushi = dict(row)
        utsushi["_dan2_chuu"] = True
        if "_moto_idx" not in utsushi:
            utsushi["_moto_idx"] = idx
        if zen and zen != row.get("_rerank_bun"):
            utsushi["_rerank_bun"] = zen
            utsushi.pop("_dan2_reuse_raw", None)
            kaeta += 1
        elif "_d1_raw" in row:
            utsushi["_dan2_reuse_raw"] = row["_d1_raw"]
        ue2.append(utsushi)
    if not kaeta:        # 全部が短い記録なら1段目と同じ物を送るだけ。呼ばない(費用の無駄)
        return naranda
    mae = dict(RERANK_JOUTAI)
    try:
        naranda2 = narabe_naosu(query, ue2, len(ue2), None, dan2=False)
    finally:
        dan2_joutai = {k: RERANK_JOUTAI.get(k) for k in ("state", "why", "datsuraku")}
        dan2_joutai["fukasa_eff"] = eff_fukasa
        dan2_joutai["fukasa_max"] = min(fukasa, len(naranda))
        RERANK_JOUTAI.clear()
        RERANK_JOUTAI.update(mae)          # 1段目の状態を上書きしない
        RERANK_JOUTAI["dan2"] = dan2_joutai
    if diagnostics is not None and isinstance(diagnostics.get("rerank"), dict):
        diagnostics["rerank"]["dan2"] = dan2_joutai
    if not naranda2 or len(naranda2) != len(ue2):
        return naranda                     # 数が合わない＝信用しない。1段目の順で返す
    # fail-open等で ue2 がそのまま返った時は1段目の順(naranda)を壊さない
    if dan2_joutai.get("state") in RERANK_SHIPPAI_JOUTAI:
        return naranda
    for row in naranda2:                   # 全文や2段目作業欄を先へ持ち出さない
        row.pop("_dan2_chuu", None)
        row.pop("_d1_raw", None)
        row.pop("_dan2_reuse_raw", None)
        key = item_key(row)
        if key in moto_bun:
            if moto_bun[key] is None:
                row.pop("_rerank_bun", None)
            else:
                row["_rerank_bun"] = moto_bun[key]
    return naranda2 + shita


SNIP_HABA = 300     # 返す切れ端の長さ。**今までと同じ**(費用を増やさない)。変えるのは切る位置だけ
SNIP_HOSA = 150     # 幅の半分ずつずらす(MAXP_MADO_HOSA と同じ比・Dai&Callan 2019 の 150語/75語)


def _rerank_bun_hagasu(rows: list[dict], query: str = "",
                       items: list | None = None) -> list[dict]:
    """リランカー専用の突き合わせ文(_rerank_bun)を応答から剥がす(2026-08-29 Code)。
    shiageで持たせた作業用の欄で、外に出すと応答が1件あたり最大1100字太る。

    ここは**全ての返り道が通る一箇所**なので、返す切れ端(snippet)を
    「本文の先頭300字」から「**問いに一番近い300字**」へ寄せるのも、ここで1回だけやる
    (2026-09-09 ネオ・出口ごとに書くと必ずどれかを書き忘れる)。

    なぜ要るか(王国の実物で実測 2026-09-09): 使う試験で「思い出せたのに使えない」が
    残り続けた真因。答えは記録の本文に在るのに、返る切れ端が**先頭300字**なので窓の外に居た。
    答えを持つ19件で、先頭300字に入っていたのは **3件だけ(16%)**。同じ300字を
    問いに寄せて切ると **11件(58%)**。長さは1字も増やさず、切る位置だけで4倍。
    世界も同じ結論で、2026年の定石は文字数で頭から切るのをやめ、問いに沿った証拠を抜く事
    (query-focused evidence extraction・arXiv:2507.15586 ほか)。

    8/29に同じ穴を一度直しているが、直したのは**リランカーへ渡す文**だけで、
    **王とAIが読む切れ端**は先頭のままだった ―― 片側だけ直すと、もう片側は10日気づかれない。
    """
    hon: dict = {}
    _terms_pre: list[str] | None = None
    if query and items and rows:
        need_eids: set[str] = set()
        for row in rows:
            if isinstance(row, dict):
                eid = str(row.get("event_id") or "") or str(row.get("id") or "").split(":")[0]
                if eid:
                    need_eids.add(eid)
        if need_eids:
            _items_id = id(items)
            _cached_hon = None
            for _entry in _HAY_INDEX_OBOE.values():
                if _entry.get("items_ref") is items:
                    _cached_hon = _entry.get("by_eid_text")
                    if _cached_hon is None:
                        _m: dict[str, str] = {}
                        for it in items:
                            _e = str(it.get("event_id") or "") or str(it.get("id") or "").split(":")[0]
                            if _e and _e not in _m:
                                _m[_e] = str(it.get("text") or "")
                        _entry["by_eid_text"] = _m
                        _cached_hon = _m
                    break
            if _cached_hon is not None:
                hon = _cached_hon
            else:
                rem = len(need_eids)
                for it in items:
                    eid = str(it.get("event_id") or "") or str(it.get("id") or "").split(":")[0]
                    if eid in need_eids and eid not in hon:
                        hon[eid] = str(it.get("text") or "")
                        rem -= 1
                        if rem <= 0:
                            break
            if hon:
                _terms_pre = [t for t in query_terms(query) if len(t) >= 2]
    for row in rows:
        if isinstance(row, dict):
            if hon:
                eid = str(row.get("event_id") or "") or str(row.get("id") or "").split(":")[0]
                raw_h = hon.get(eid, "")
                if len(raw_h) > SNIP_HABA:
                    honbun = " ".join(raw_h.split())
                    if len(honbun) > SNIP_HABA:
                        row["snippet"] = saiyo_mado(query, honbun,
                                                    haba=SNIP_HABA, hosa=SNIP_HOSA,
                                                    _terms_pre=_terms_pre)
            row.pop("_rerank_bun", None)
            row.pop("_text_zenbun", None)     # MaxPの作業欄(2026-09-05)も出さない
            row.pop("_zenbun_yo", None)       # 2段目の作業欄(2026-09-14)も出さない
            row.pop("_moto_idx", None)        # 2段目の位置項の作業欄(2026-11-15)も出さない
            row.pop("_dan2_chuu", None)
            row.pop("_d2_raw", None)
            row.pop("_d2_bonus", None)
            row.pop("_item_ref", None)
    return rows


def sonzai_shiteita(item: dict, as_of: datetime) -> bool:
    """その時刻に、この記録は**もう在ったか**。

    世界の言葉で言えば transaction time(いつ台帳に載ったか)での絞り込み。
    訂正の valid_from/valid_until は valid time(いつ本当だったか)で、こちらは
    _window_active が既に見ている。二つを合わせて bitemporal(SQL:2011 と同じ二軸)。
    時刻を持たない記録(道具台帳・話題md等)は**落とさない**。落とした数は呼び手へ返す。
    """
    when = _iso_datetime(str(item.get("timestamp") or ""))
    if when is None:
        return True
    ima, toki = as_of, when
    if ima.tzinfo is None or toki.tzinfo is None:
        ima, toki = ima.replace(tzinfo=None), toki.replace(tzinfo=None)
    return toki <= ima


def as_of_items(items: list[dict], as_of: datetime | None,
                diagnostics: dict | None = None) -> list[dict]:
    """通常検索と運用検索の共通の時点フィルタ（候補数も残す）。"""
    if as_of is None:
        return items
    kept = [item for item in items if sonzai_shiteita(item, as_of)]
    if diagnostics is not None:
        diagnostics["as_of"] = {
            "toki": as_of.isoformat(timespec="seconds"),
            "moto_kensuu": len(items), "nokotta": len(kept),
            "hazushita": len(items) - len(kept),
            "jikoku_nashi_nokoshita": sum(1 for i in items if not str(i.get("timestamp") or "")),
        }
    return as_of_correction_items(kept, as_of, diagnostics)


def _correction_known_at(window: dict, as_of: datetime) -> str:
    """Ledger transaction time is separate from valid_from. Unknown is withheld."""
    recorded = _iso_datetime(str(window.get("recorded_at") or ""))
    event = _iso_datetime(str(window.get("correcting_event_timestamp") or ""))
    if as_of.tzinfo is None:
        # 時差の無い as_of(CLI の例 2026-08-15 等)は日本時間として読む。
        # _event_time_key・nirai_kotae.jiten_wo_yomu と同じ規則(2026-10-31 retr-3)。
        as_of = as_of.replace(tzinfo=timezone(timedelta(hours=9)))
    # A demonstrably future event alone is already sufficient to exclude it.
    if event is not None and event.tzinfo is not None and event > as_of:
        return "future"
    if recorded is None or recorded.tzinfo is None:
        return "unknown"
    if event is not None and event.tzinfo is None:
        return "unknown"
    return "known" if recorded <= as_of else "future"


def as_of_correction_items(items: list[dict], as_of: datetime,
                            diagnostics: dict | None = None) -> list[dict]:
    """Remove correction metadata not knowable as-of, including chain targets.

    Missing transaction timestamps are not invented from valid_from. This is
    a read-time projection only; canonical ledgers and snapshot stay intact.
    """
    out, known_windows = [], []
    counts = {"future": 0, "unknown": 0}
    for item in items:
        row = dict(item)
        removed = False
        for field in ("supersession_windows", "kin_windows", "moto_ga_teisei_sareta"):
            kept = []
            for window in row.get(field) or []:
                if not isinstance(window, dict):
                    counts["unknown"] += 1; removed = True; continue
                state = _correction_known_at(window, as_of)
                if state != "known":
                    counts[state] += 1; removed = True; continue
                window = dict(window)
                # Chain endpoints computed from the unfiltered ledger may be future.
                window.pop("kusari_owari", None)
                kept.append(window)
                if field == "supersession_windows" and _window_active(
                        str(window.get("valid_from") or ""),
                        str(window.get("valid_until") or ""), as_of):
                    known_windows.append((str(row.get("event_id") or row.get("id") or ""), window))
            if field in row:
                row[field] = kept
        if row.get("correction_id"):
            state = _correction_known_at({"recorded_at": row.get("correction_recorded_at"),
                "correcting_event_timestamp": row.get("correction_event_timestamp")}, as_of)
            if state != "known":
                counts[state] += 1; removed = True
                for field in list(row):
                    if field.startswith("correction_") or field == "corrects":
                        row.pop(field, None)
        if removed:
            row.pop("claim_key", None)
            for field in SEISAKU_NO_RAN:
                row.pop(field, None)
        out.append(row)
    # Rebuild only the already-known, valid chain; no endpoint or summary from
    # the future can survive the snapshot's precomputed correction chain.
    chain = teisei_no_kusari([dict(window, targets=[{"kind": "event", "value": target}])
                            for target, window in known_windows if target])
    for row in out:
        for field in ("supersession_windows", "kin_windows"):
            for window in row.get(field) or []:
                end = chain.get(str(window.get("correcting_event_id") or ""))
                if end:
                    window["kusari_owari"] = end
    if diagnostics is not None:
        diagnostics.setdefault("as_of", {}).update(
            correction_future_withheld=counts["future"],
            correction_transaction_unknown_withheld=counts["unknown"])
    return out


def search_snapshot(snapshot: dict, query: str, participant: str = "", limit: int = 3,
                    root: Path | None = None, participant_scope: str = "auto",
                    diagnostics: dict | None = None,
                    as_of: datetime | None = None,
                    brain_mode: str = "") -> list[dict]:
    eff_mode = resolve_brain_mode(brain_mode)
    with _cpu_agent_scope(eff_mode == "cpu_agent"):
        return _search_snapshot_impl(
            snapshot, query, participant, limit, root, participant_scope,
            diagnostics, as_of, eff_mode)


def _search_snapshot_impl(snapshot: dict, query: str, participant: str = "", limit: int = 3,
                          root: Path | None = None, participant_scope: str = "auto",
                          diagnostics: dict | None = None,
                          as_of: datetime | None = None,
                          eff_mode: str = "auto") -> list[dict]:
    is_cpu_agent = (eff_mode == "cpu_agent")
    if diagnostics is not None:
        diagnostics.clear()
        diagnostics.update({
            "path": "regular",
            "brain_mode": eff_mode,
            "semantic": {"state": ("disabled_cpu_agent" if is_cpu_agent else "unknown"),
                         "why": ("cpu_agent_mode" if is_cpu_agent else "")},
            "rerank": {"state": ("disabled_cpu_agent" if is_cpu_agent else "unknown"),
                       "why": ("cpu_agent_mode" if is_cpu_agent else "")},
        })
    operational = _operational_search(
        snapshot, query, participant, participant_scope, limit, as_of, diagnostics)
    if operational:                 # 2026-08-31 修理(バグ狩りC28): 0件なら通常検索へ落とす
        _rerank_joutai_kaku("not_required", "canonical_event_query")
        if diagnostics is not None:
            diagnostics.update({
                "path": "operational",
                "semantic": {"state": "not_required", "why": "canonical_event_query"},
                "rerank": {"state": "not_required", "why": "canonical_event_query"},
            })
        return _rerank_bun_hagasu(operational)
    terms = query_terms(query)
    ito = jikan_ito(query)          # 問いが向いている時間(今 / 過去の一点 / 無し)
    results = []
    items = as_of_items(searchable_items(snapshot), as_of, diagnostics)
    hays, avgdl = haystacks_of(snapshot, items, root)
    scope = _participant_scope(query, participant, participant_scope)
    # BM25: 珍しい語ほど重く(IDF)、長い文書を有利にしない(長さ正規化)、同じ語の連発は頭打ち(飽和)。
    # 2026-08-20 実測でこれが要ると分かった: IDFが無いため「システム」(カタカナ3つの2gram=30点)が
    # 「帳票」(1語=4点)に勝ち、帳票の問いで無関係な長い診断ログが正解より上に立っていた。
    total = max(1, len(items))
    idx_cache = _hay_index_for(hays, avgdl, items)
    bm25_scores = _bm25_taat_scores(hays, avgdl, terms, total, idx_cache)
    use_meta = idx_cache is not None and idx_cache.get("items_ref") is items
    static_omake = idx_cache["static_omake"] if use_meta else None
    parsed_ts = idx_cache["parsed_ts"] if use_meta else None
    has_corr = idx_cache["has_corr"] if use_meta else None
    shiage_bucket = idx_cache["shiage_bucket"] if idx_cache is not None else None
    now_utc = datetime.now(timezone.utc) if ito != "kako" else None
    now_naive = datetime.now() if ito != "kako" else None
    check_part = bool(participant)
    _fold_cap = max(1, min(limit, 100), RERANK_KOUHO, dan1_kouho())
    _mat_cap = max(250, _fold_cap * 5)
    _use_lazy = (
        lazy_shiage_ari()
        and len(bm25_scores) > max(1000, _mat_cap)
        and imi_wo_mazeru is _ORIG_IMI_WO_MAZERU
        and shiage is _ORIG_SHIAGE
        and fukusuu_thread_de_hirou is _ORIG_FUKUSUU_THREAD
        and kanten_de_hirou is _ORIG_KANTEN_HIROU
        and shudai_de_hirou is _ORIG_SHUDAI_HIROU
    )
    scored_rows: list[tuple[float, str, int, float]] = []
    # sorted(bm25_scores) preserves exact ascending item-index order for stable tie-breaking.
    for index in sorted(bm25_scores):
        term_score = bm25_scores[index]
        # 語が1つも当たっていない物を、鮮度や要約の加点だけで上位へ来させない。
        if term_score <= 0:
            continue
        item = items[index]
        if not _participant_matches(item, participant, scope):
            continue
        omake = static_omake[index] if use_meta else 0.0
        if not use_meta:
            trust = item.get("trust")
            if trust in {"verified_record", "verified_report", "resolved"}:
                omake += 0.8
            elif trust == "measured_claim":
                omake += 0.4
            if str(item.get("proof") or "") == "王合格":
                omake += 1.0   # 王の合格は王国の金印
            if item.get("digest"):
                omake += 0.5   # 話題の3行要約は「今どこ」の答え
        if check_part and (_onaji_hontai(item.get("sender"), participant)
                           or _recipient_matches(str(item.get("to") or ""), participant)):
            omake += 0.3
        if ito != "kako":
            if (has_corr[index] if use_meta else correction_current(item)) and correction_matches(item, query):
                omake += 1.2
            if use_meta:
                when = parsed_ts[index][1]
            else:
                try:
                    when = datetime.fromisoformat(str(item.get("timestamp") or ""))
                except (ValueError, TypeError):
                    when = None
            if when is not None:
                age_days = ((now_utc - when) if when.tzinfo is not None else (now_naive - when)).days
                if age_days <= 7:
                    omake += 0.6    # 新しい記録を上へ
                elif age_days <= 30:
                    omake += 0.3
        score = term_score + omake
        if score <= 0:
            continue
        if _use_lazy:
            ts_str = parsed_ts[index][0] if use_meta else str(item.get("timestamp") or "")
            scored_rows.append((round(float(score), 2), ts_str, index, score))
        else:
            results.append(shiage(item, score, shiage_bucket, with_rerank_fields=False))

    imi_best_cnum: dict[str, int] = {}
    shudai_kagi_uke: list = []
    if _use_lazy:
        scored_rows.sort(key=lambda t: (t[0], t[1]), reverse=True)
        _ik = idx_cache["item_keys"] if use_meta else None
        _bk = idx_cache["by_key"] if use_meta else None
        _imi_root = None if is_cpu_agent else root
        results, tail_ctx, all_exist_keys, _lazy_flags = _speed_cache.imi_wo_mazeru_lazy_impl(
            sys.modules[__name__], _imi_root, query, items, scored_rows, _ik, shiage_bucket,
            diagnostics, imi_best_cnum, _mat_cap, by_key_hint=_bk,
        )
        if is_cpu_agent and diagnostics is not None:
            diagnostics["semantic"] = {"state": "disabled_cpu_agent", "why": "cpu_agent_mode"}
        _t_ord, _t_scored, _t_sem, _t_keys, _t_chikasa, _t_flags = tail_ctx
        _n_moji = len(_t_scored)
        def _tail_item_of(oi: int) -> dict:
            return items[_t_scored[oi][2]] if oi < _n_moji else _t_sem[oi - _n_moji][1]
        def _mat_tail_oi(oi: int) -> dict:
            if oi < _n_moji:
                t = _t_scored[oi]
                r = shiage(items[t[2]], t[3], shiage_bucket, with_rerank_fields=False)
            else:
                r = shiage(_t_sem[oi - _n_moji][1], 0)
            k = _t_keys[oi]
            if k in _t_chikasa:
                r["imi"] = round(_t_chikasa[k], 3)
            if _t_flags:
                r.update(_t_flags)
            return r
        results = fukusuu_thread_de_hirou(
            query, items, results, diagnostics,
            _all_existing_keys=all_exist_keys,
            _lazy_tail_iter_fn=(lambda: (_tail_item_of(oi) for oi in _t_ord)),
        )
        all_exist_keys.update(item_key(r) for r in results[_mat_cap:])
        results = kanten_de_hirou(
            query, items, hays, avgdl, results, diagnostics,
            _all_existing_keys=all_exist_keys,
        )
        _tail_added = results[_mat_cap:]
        results = results[:_mat_cap]
        def _promote_from_tail(motome_eids: set[str]) -> list[dict]:
            promoted = []
            if _t_ord:
                kept = []
                for oi in _t_ord:
                    it = _tail_item_of(oi)
                    eid = str(it.get("event_id") or "") or str(it.get("id") or "").split(":")[0]
                    if eid in motome_eids:
                        promoted.append(_mat_tail_oi(oi))
                    else:
                        kept.append(oi)
                if promoted:
                    _t_ord[:] = kept
            if _tail_added:
                kept_add = []
                for r in _tail_added:
                    eid = str(r.get("event_id") or "") or str(r.get("id") or "").split(":")[0]
                    if eid in motome_eids:
                        promoted.append(r)
                    else:
                        kept_add.append(r)
                if len(kept_add) != len(_tail_added):
                    _tail_added[:] = kept_add
            return promoted
        results = shudai_de_hirou(
            root, query, items, results, diagnostics,
            kagi_uke=shudai_kagi_uke, _lazy_promote_fn=_promote_from_tail,
        )
    else:
        # 2026-08-31 修理(バグ狩りC2): int()が小数加点(検証済+0.8/王合格+1.0等)を捨て、
        # 同じ整数バケツ内で新しい未検証が検証済に勝っていた。floatのまま並べる。
        results.sort(key=lambda item: (float(item.get("score") or 0.0), str(item.get("timestamp") or "")), reverse=True)
        if is_cpu_agent:
            if diagnostics is not None:
                diagnostics["semantic"] = {"state": "disabled_cpu_agent", "why": "cpu_agent_mode"}
        else:
            results = imi_wo_mazeru(root, query, items, results, diagnostics, chunk_pos_out=imi_best_cnum)
        # 複数観点の問いだけ、最初に見つけた記録の会話糸を1-hop辿る。
        results = fukusuu_thread_de_hirou(query, items, results, diagnostics)
        # 問いを観点A/Bに割り、各観点の一位を候補へ確保する（2026-09-13 ネオ ai-9749・サカナAIの設計の第2段）
        results = kanten_de_hirou(query, items, hays, avgdl, results, diagnostics)
        # 問いが名指しした主題を、意味検索に押し下げられたまま終わらせない（完全一致の車線）
        results = shudai_de_hirou(root, query, items, results, diagnostics,
                                  kagi_uke=shudai_kagi_uke)
    # 「意味では近い記録が1件も無い」は問い全体の性質。意味の後に拾い足した車線(糸・観点・主題)の
    # 行にも同じ印を付ける(付けないと、並びが動いた時に印の無い行が1位に立ち「無い」と言えなくなる)。
    nashi_moto = next((row for row in results if row.get("chikai_kiroku_nashi")), None)
    if nashi_moto is not None:
        for row in results:
            if not row.get("chikai_kiroku_nashi"):
                row["chikai_kiroku_nashi"] = True
                if nashi_moto.get("moji_de_atatteru") is not None:
                    row["moji_de_atatteru"] = nashi_moto["moji_de_atatteru"]
    if scope != "boost":
        results = [row for row in results if _participant_matches(row, participant, scope)]
    # 同じ中身が並ぶのを畳む。2026-08-20 実測: 解決台帳へ同じ回答を複数の質問に紐付けると、
    # semantic層に同じ文が何件も立ち、検索結果の1位と2位が同じ物になっていた。
    folded, seen, mita_id = [], set(), set()
    def _fold_one(row: dict) -> bool:
        event_identity = str(row.get("event_id") or row.get("answer_event_id") or "")
        rid = str(row.get("id") or event_identity or "")
        if rid:
            if rid in mita_id:
                return False
            mita_id.add(rid)
        provenance = event_identity or str(row.get("report_path") or row.get("path") or "")
        key = (provenance, sha_text(" ".join(str(row.get("summary") or "").split())[:120]
                       + "|" + " ".join(str(row.get("snippet") or "").split())[:200]))
        if key in seen:
            return False
        seen.add(key)
        folded.append(row)
        return len(folded) >= _fold_cap

    for row in results:
        if _fold_one(row):
            break
    if _use_lazy and len(folded) < _fold_cap:
        for oi in _t_ord:
            row = _mat_tail_oi(oi)
            if nashi_moto is not None and not row.get("chikai_kiroku_nashi"):
                row["chikai_kiroku_nashi"] = True
                if nashi_moto.get("moji_de_atatteru") is not None:
                    row["moji_de_atatteru"] = nashi_moto["moji_de_atatteru"]
            if scope != "boost" and not _participant_matches(row, participant, scope):
                continue
            if _fold_one(row):
                break
        if len(folded) < _fold_cap and _tail_added:
            for row in _tail_added:
                if nashi_moto is not None and not row.get("chikai_kiroku_nashi"):
                    row["chikai_kiroku_nashi"] = True
                    if nashi_moto.get("moji_de_atatteru") is not None:
                        row["moji_de_atatteru"] = nashi_moto["moji_de_atatteru"]
                if scope != "boost" and not _participant_matches(row, participant, scope):
                    continue
                if _fold_one(row):
                    break
    kazu = max(1, min(limit, 100))
    _rr_cap = max(kazu, RERANK_KOUHO, dan1_kouho())
    for row in folded[:_rr_cap]:
        _ref = row.pop("_item_ref", None)
        if _ref is not None and "_rerank_bun" not in row:
            _attach_rerank_fields(row, _ref)
    for row in folded[_rr_cap:]:
        row.pop("_item_ref", None)
    if root is not None and len(folded) > 1 and not is_cpu_agent:
        # 最後に交差符号化リランカーで並べ直す(2026-08-21)。居なければ素通りする。
        # 2026-08-29: リランカーの後ろで「時間の意味」を当てるため、kazu ではなく
        # 候補を広く受け取る。リランカーは時間もtrustも見ないので、ここが最後の砦。
        naranda = narabe_naosu(
            query, folded, max(kazu, RERANK_KOUHO), diagnostics,
            best_cnum=imi_best_cnum, idx_cache=idx_cache, hays=hays,
            total_docs=total, enable_evolve=True)
        shudai = shudai_as_of_ni_shiboru(shudai_wo_yomu(root), snapshot, as_of)
        naranda = shudai_seisaku(
            yuukou_seisaku(jikan_seisaku(naranda, ito, as_of), ito, as_of, query=query),
            ito, shudai)
        # 2026-11-21 ルートA-3: 規模門汎用改善（仮説B: 単発 話題/*.md ミラー解決、仮説A: 同型テンプレート最新代表化）
        naranda = _a3_wadai_mirror_resolve(naranda)
        naranda = _a3_template_latest_promote(naranda, query, ito)
        kouho = yuukou_seisaku(folded[:RERANK_KOUHO], ito, as_of, query=query)
        # 2026-08-31 修理(バグ狩りC10): 席の資格は「候補50件の中に居る」(docstringの契約)。
        # folded全体から拾うと204位の圏外記録でも最終席に座っていた。
        # 2026-09-05 修理(バグ探し高#4): 道具型の問いは道具の席を先頭に。その後で王の席(最後)。
        # 2026-09-13 ネオ ai-9749: 観点の席は**最後**に適用する（王・道具・主題の席は奪わない）。
        #   リランカーが蹴り落とした観点の代表を、末尾の席で守る。
        out = _rerank_de_nashi_wo_kaijo(
            _rerank_bun_hagasu(supersedes_no_seki(
                shudai_shirushi_wo_tsukeru(
                    kanten_no_seki(
                        shudai_no_seki(
                            ou_no_seki(dougu_no_seki(naranda, kouho, kazu, query), kouho, kazu, query),
                            kouho, kazu, shudai_kagi_uke, diagnostics, query=query),
                        kouho, kazu, items, hays, avgdl, query, root, diagnostics,
                        ito=ito, as_of=as_of), shudai),
                items, kazu, query, ito, as_of, participant, scope),
                query, items),
            diagnostics)
        _attach_spec_coverage_diag(query, out, items, diagnostics)
        return out
    if is_cpu_agent:
        if diagnostics is not None:
            diagnostics["rerank"] = {"state": "disabled_cpu_agent", "why": "cpu_agent_mode"}
        _shudai = shudai_as_of_ni_shiboru(shudai_wo_yomu(root), snapshot, as_of)
        _cpu_ranked = shudai_seisaku(
            yuukou_seisaku(jikan_seisaku(folded, ito, as_of), ito, as_of, query=query),
            ito, _shudai)
        _cpu_kouho = yuukou_seisaku(folded[:max(kazu, RERANK_KOUHO)], ito, as_of, query=query)
        out = _rerank_bun_hagasu(supersedes_no_seki(
            shudai_shirushi_wo_tsukeru(
                kanten_no_seki(
                    shudai_no_seki(
                        ou_no_seki(dougu_no_seki(_cpu_ranked, _cpu_kouho, kazu, query), _cpu_kouho, kazu, query),
                        _cpu_kouho, kazu, shudai_kagi_uke, diagnostics, query=query),
                    _cpu_kouho, kazu, items, hays, avgdl, query, None, diagnostics,
                    ito=ito, as_of=as_of, cpu_only=True),
                _shudai),
            items, kazu, query, ito, as_of, participant, scope), query, items)
        _attach_spec_coverage_diag(query, out, items, diagnostics)
        _attach_cpu_agent_confidence_diag(query, terms, out, items, total, idx_cache, diagnostics)
        return out
    _rerank_joutai_kaku("not_needed", "candidate_count_lt_2")
    if diagnostics is not None:
        diagnostics["rerank"] = {"state": "not_needed", "why": "candidate_count_lt_2"}
    _shudai = shudai_as_of_ni_shiboru(shudai_wo_yomu(root), snapshot, as_of)
    out = _rerank_bun_hagasu(supersedes_no_seki(
        shudai_shirushi_wo_tsukeru(
            shudai_seisaku(
                yuukou_seisaku(jikan_seisaku(folded, ito, as_of), ito, as_of, query=query),
                ito, _shudai)[:kazu], _shudai),
        items, kazu, query, ito, as_of, participant, scope), query, items)
    _attach_spec_coverage_diag(query, out, items, diagnostics)
    return out


# ⚠️ 覚え書きの鍵は「どのファイルか」まで含める(2026-09-09 ai-0f 指摘)。
#    凍結土俵は本番から mtime を保つ形で写すので、時刻だけを鍵にすると
#    本番と凍結を同じプロセスで交互に引いた時、**凍結が本番の主題の表を返す**。
_SHUDAI_OBOE: dict = {"kagi": None, "hyou": {}}

# 2026-09-13 ネオ ai-963e（fail-open 62箇所の選り分け・第一歩）:
#   facts.jsonl(訂正の主題台帳)が「壊れている／今は読めない」時、この関数は空の {} を返し、
#   検索は**訂正が1件も無い世界**として黙って動いていた(⏮「これより新しい記録がある」の印が
#   全部消える)。訂正済みの古い記録が、訂正されていない顔で王の前に出る。
#   ★王命「止めるな」は守る ―― 止めない。**読めなかった事を答えに載せる**だけ。
#   状態は5つに分ける(くくるP0の「OK/無い/壊れている/今は読めない/古い」):
#       ok / nai / kowareteiru / yomenai / kirareteiru
#   ⚠️答えに載せるのは kowareteiru と yomenai の2つだけ。
#     ・nai と 古い は sou_no_okure() が既に見ている(SOU に facts が在る)
#     ・kirareteiru は測定者が自分で立てた切り分けの口。ここで欄を足すと
#       「入れた前/入れた後」で骨組みの太さが変わり、**測定そのものを汚す**
#     ・空欄を常に載せると骨組みが太り狭い予算の試験が落ちる(このファイルで2度踏んだ)
_SHUDAI_JOUTAI: dict = {"state": "unknown", "naze": ""}


def _shudai_joutai(state: str, naze: str = "", **soe) -> None:
    """主題台帳を読めたかを憶える(判定も順位も変えない。憶えるだけ)。"""
    _SHUDAI_JOUTAI.clear()
    _SHUDAI_JOUTAI.update({"state": state, "naze": naze})
    _SHUDAI_JOUTAI.update(soe)


def shudai_wo_yomu(root: Path | None) -> dict:
    """fact層が束ねた主題を読む(facts.jsonl・更新時刻で覚えるので毎回は読まない)。

    返す形: {event_id: {"rank": "3/7", "newer": [event_id...], "keys": [主題...]}}
    facts.jsonl が無い・壊れている時は空を返す ―― **読めない事を理由に検索を止めない**。
    ただし**黙らない**。読めたかどうかは _SHUDAI_JOUTAI に憶え、
    `shudai_yomi_keikoku()` 経由で freshness に一言だけ載る(2026-09-13 ネオ ai-963e)。
    """
    if root is None:
        _shudai_joutai("nai", "rootが無い")
        return {}
    # 2026-09-09 ネオ: 切り分けの口。SHUDAI_SEISAKU=0 で主題の政策を丸ごと素通りさせる。
    # 「入れた前」と「入れた後」を同じ土俵で測るために要る ―― 自分で受入条件に
    # 「凍結土俵で R@1 が下がらないこと」と書いた以上、切って測れる形にしておく。
    if os.environ.get("SHUDAI_SEISAKU", "1") == "0":
        _shudai_joutai("kirareteiru", "SHUDAI_SEISAKU=0(測る人が自分で切った)")
        return {}
    p = Path(root) / "roundtable" / "brain" / "facts.jsonl"
    try:
        mtime = p.stat().st_mtime
    except FileNotFoundError:
        # 「無い」は sou_no_okure() が既に見ている層。ここでは二重に鳴らさない。
        _shudai_joutai("nai", "facts.jsonl が無い")
        return {}
    except OSError as e:
        _shudai_joutai("yomenai", "stat できない: %s" % e)
        return {}
    oboe_kagi = (str(p), mtime)
    if _SHUDAI_OBOE["kagi"] == oboe_kagi:
        # 覚えている時も状態を戻す(覚えた時の状態がそのまま今の状態)
        _SHUDAI_JOUTAI.clear()
        _SHUDAI_JOUTAI.update(_SHUDAI_OBOE.get("joutai") or {"state": "ok", "naze": ""})
        return _SHUDAI_OBOE["hyou"]
    hyou: dict = {}
    # 2026-09-12 くくる監査③F: 不正行を無言 continue すると facts.jsonl の一部が
    #   欠けたまま「正常キャッシュ」される。破損を数え、stderr に警告するのみで
    #   検索は続行する(docstring の「読めない事を理由に検索を止めない」を守る)。
    corrupted = 0
    try:
        with p.open(encoding="utf-8") as f:
            for lineno, line in enumerate(f, start=1):
                line = line.strip()
                if not line:
                    continue
                try:
                    row = json.loads(line)
                except json.JSONDecodeError:
                    corrupted += 1
                    if not _SOTO_NI_DASANAI.get():      # ④ 囲いの行の頭を王国の記録へ出さない
                        sys.stderr.write(
                            f"[brain:shudai_wo_yomu] JSONDecodeError at {p}:{lineno} "
                            f"head={line[:80]!r}\n")
                    continue
                newer = row.get("claim_newer") or []
                is_disputed = str(row.get("status") or "") == "disputed"
                if not newer and not is_disputed:
                    continue
                eid = str(row.get("event_id") or "")
                if eid:
                    entry = {"rank": str(row.get("claim_rank") or ""),
                             "newer": [str(x) for x in newer],
                             "keys": [str(x) for x in (row.get("claim_keys") or [])]}
                    if is_disputed:
                        entry["disputed"] = True
                        entry["disputed_reasons"] = [str(x) for x in (row.get("reasons") or [])[:3]]
                    hyou[eid] = entry
    except OSError as e:
        _shudai_joutai("yomenai", "開けない・途中で読めなくなった: %s" % e)
        return {}
    if corrupted and not _SOTO_NI_DASANAI.get():
        sys.stderr.write(
            f"[brain:shudai_wo_yomu] {p.name} に {corrupted} 行の不正JSON。"
            f"主題政策が部分的に落ちている可能性あり(表={len(hyou)}件で続行)\n")
    if corrupted:
        _shudai_joutai("kowareteiru", "壊れた行 %d 行" % corrupted,
                       kowareta_gyou=corrupted, yometa=len(hyou))
    else:
        _shudai_joutai("ok", "", yometa=len(hyou))
    _SHUDAI_OBOE["kagi"] = oboe_kagi
    _SHUDAI_OBOE["hyou"] = hyou
    _SHUDAI_OBOE["joutai"] = dict(_SHUDAI_JOUTAI)
    return hyou


def shudai_yomi_keikoku(root: Path | None) -> dict:
    """訂正の主題台帳(facts.jsonl)を読めたか。**異常な時だけ**中身が入る(正常なら {})。

    止めない・順位も変えない。読む側(AI と王)が
    「⏮の印が消えているのは『新しい記録が無い』からではないかもしれない」と
    分かるための一言だけを作る。2026-09-13 ネオ ai-963e。
    """
    shudai_wo_yomu(root)          # 状態を今にする(mtime で覚えているので安い)
    j = dict(_SHUDAI_JOUTAI)
    state = str(j.get("state") or "")
    if state == "yomenai":
        j["keikoku"] = (
            "⚠️訂正の主題台帳(facts.jsonl)が**今は読めない**(%s)。"
            "**この検索に⏮『これより新しい記録がある』の印が出ていない事は、"
            "『新しい記録が無い』という意味にならない。**"
            "訂正済みの古い記録が、訂正されていない顔で出ている可能性がある。" % j.get("naze"))
        return j
    if state == "kowareteiru":
        j["keikoku"] = (
            "⚠️訂正の主題台帳(facts.jsonl)に壊れた行が%d行ある(読めた%d件で続行)。"
            "⏮の印が**一部欠けている**可能性がある。" % (
                int(j.get("kowareta_gyou") or 0), int(j.get("yometa") or 0)))
        return j
    return {}


def shudai_shirushi(info: dict) -> str:
    """読む側(AI)が一目で「これは何世代前か」を分かる1行。"""
    newer = info.get("newer") or []
    keys = "・".join(info.get("keys") or [])[:60]
    return ("⏮同じ物について、これより新しい記録が%d件ある(主題=%s／順位%s・1が最新)。"
            "今を問う答えには新しい方を使う" % (len(newer), keys or "?", info.get("rank") or "?"))


def shudai_shirushi_wo_tsukeru(rows: list[dict], shudai: dict | None) -> list[dict]:
    """最終の並びへ⏮の印だけ付ける(**順位は動かさない**)。

    2026-09-09 実測で分かった事: 並べ替えを naranda(リランク直後)に当てても、
    その後の dougu_no_seki / ou_no_seki が候補(kouho)から行を取り直すので、
    最終の応答には印の付いていない行が混ざる ―― 印が付いた行だけを見て
    「効いている」と言うと嘘になる。だから**印は出口で1回、全行へ**付ける(教訓9「入口は1つ」)。
    並べ替えは今までどおり shudai_seisaku が段の中で行い、ここでは触らない。
    """
    if not rows or not shudai:
        return rows
    for row in rows:
        if not isinstance(row, dict) or row.get("shudai_shirushi"):
            continue
        info = shudai.get(str(row.get("event_id") or row.get("id") or ""))
        if not info:
            continue
        row["shudai_rank"] = info.get("rank", "")
        row["shudai_newer"] = list(info.get("newer") or [])[:5]
        row["shudai_keys"] = list(info.get("keys") or [])[:5]
        row["shudai_shirushi"] = _shudai_shirushi_with_disputed(info)
        if info.get("disputed"):
            row["shudai_disputed"] = True
            row["shudai_disputed_reasons"] = list(info.get("disputed_reasons") or [])[:3]
    return rows


def _shudai_shirushi_with_disputed(info: dict) -> str:
    parts = []
    if info.get("newer"):
        parts.append(shudai_shirushi(info))
    if info.get("disputed"):
        keys = "・".join(info.get("keys") or [])[:60]
        reasons = " / ".join(str(x) for x in (info.get("disputed_reasons") or []) if str(x).strip())[:120]
        parts.append(
            "[注意:相反(disputed)] 同じ主題の最新が競合し機械では1つに決められない(主題=%s%s)。"
            "双方の原本を確かめて判断する" % (keys or "?", ("・" + reasons) if reasons else "")
        )
    return " ".join(parts)


# 政策(最下段へ回す)を効かせてよい主題キーの種類(2026-09-09 ネオ ai-9598)。
# 実測(束ね組を無作為に取り、全部この目で読んだ。LLM判定は使っていない):
#   anken: 誤爆 0/6組(全組) ／ oumei: 0/5組 ／ file: 5/10組(50%) ／ svc: 4/5組(80%) ／ port: 5/5組(100%)
#   例) port:4416 の3件 = もののけ姫の実写化 / ComfyUIをv0.33.2へ / AI仙人ch全63本の教科書。3つとも無関係
#       port:11434 の19件 = 「ollamaを使った話」というだけで19件が1つの主題になっていた
#       file:他窓への依頼_20260907_くくる.md の9件 = 全部が別々の仕事の縄張り宣言
# 線引き: **主題そのものの名前(王命番号・案件ID)は同じ主張の系列を作る。
#          触れた物の名前(ファイル・サービス・ポート)は系列を作らない。**
# 裏付け(独立した2つの測り方が同じ所を指した): ai-0f が凍結土俵276問(base-20260828-facts)で
#   「file: だけ政策を切る」を測り、R@1 0.546 / R@3 0.721 / MRR@10 0.639 で**政策なしと3桁とも完全一致**。
#   ＝誤爆は100% file: 由来。ai-0f が実物2件まで降りて機序を掴み(正解が1位→44位)、私が率にした。
# ⚠️ **効いているとは書かない。** 同じ276問で oumei:/anken: の政策は1問も動かしていない
#   (③が①と完全一致した、という事はそういう意味でもある ―― ai-0f の指摘)。
#   これは「良くする」変更ではなく、**害が確認された所だけ止める**変更。
# ⚠️ 近似である事も書いておく: claim_newer は「どのキーで束ねられたか」を区別しないので、
#   ここは「その記録が政策キーを1つでも持つか」で判定している。正確にやるなら fact_layer 側で
#   キー種別ごとに newer を分ける必要があり、それは facts.jsonl のスキーマ変更(未着手)。
# 切り分けの口: SHUDAI_SEISAKU_KAGI="" で昔どおり全キーに効かせられる(測るために要る)。
_SEISAKU_KAGI_KITEI = "oumei:,anken:"


def seisaku_no_kiku_kagi(keys) -> bool:
    """この記録の主題キーに、政策を効かせてよい種類が1つでも在るか。

    在れば最下段へ回してよい。無ければ**印だけ付けて順位は動かさない**。
    キーが1つも無い記録は今までどおり判定しない(安全側)。
    """
    shu = tuple(k.strip() for k in os.environ.get(
        "SHUDAI_SEISAKU_KAGI", _SEISAKU_KAGI_KITEI).split(",") if k.strip())
    if not shu:                      # 空指定 = 昔どおり全部に効かせる(測定用)
        return True
    return any(str(k).startswith(shu) for k in (keys or []))


def shudai_as_of_ni_shiboru(shudai: dict | None, snapshot: dict | None,
                            as_of: datetime | None) -> dict | None:
    """as_of(過去時点)の検索では、主題の「これより新しい記録」もその時点までに絞る。

    2026-10-31 バグ探し retr-5: shudai_wo_yomu は今の facts.jsonl 全体の表で、as_of を知らない。
    そのまま使うと「その時点より後に生まれた記録」を理由に 順位の印・順位(shudai_rank)が付き、
    「今」を問う文では当時の最新記録が最下段へ回される。as_of の契約は
    「その時刻より後に生まれた記録は候補から外す」なので、newer も同じ線で切る。
    時刻は snapshot の episodic の timestamp、無ければ event_id の2〜11桁目(epoch秒・先頭は0)。
    どちらも読めない物は「その時点に在ったと言えない」ので落とす。
    元の表は書き換えない(キャッシュ共有のため)。as_of が無ければ素通り。
    """
    if as_of is None or not shudai:
        return shudai
    if as_of.tzinfo is None:    # 時差の無い as_of は日本時間(_correction_known_at と同じ約束)
        as_of = as_of.replace(tzinfo=timezone(timedelta(hours=9)))
    jikoku: dict[str, str] = {}
    for r in ((snapshot or {}).get("memory") or {}).get("episodic") or []:
        if isinstance(r, dict) and r.get("event_id"):
            jikoku[str(r["event_id"])] = str(r.get("timestamp") or "")

    def _umareta(eid: str) -> datetime | None:
        t = _iso_datetime(jikoku.get(eid, ""))
        if t is None and re.match(r"^0\d{10}", eid):
            try:
                t = datetime.fromtimestamp(int(eid[1:11]), timezone.utc)
            except (OverflowError, OSError, ValueError):
                t = None
        if t is not None and t.tzinfo is None:
            t = t.replace(tzinfo=as_of.tzinfo)
        return t

    out: dict = {}
    for eid, info in shudai.items():
        newer = list(info.get("newer") or [])
        kept = []
        for n in newer:
            t = _umareta(str(n))
            if t is not None and (as_of.tzinfo is None or t.tzinfo is not None) and t <= as_of:
                kept.append(n)
        if len(kept) == len(newer):
            out[eid] = info
            continue
        if not kept:
            if info.get("disputed"):
                te = _umareta(str(eid))
                if te is None or ((as_of.tzinfo is None or te.tzinfo is not None) and te <= as_of):
                    new = dict(info)
                    new["newer"] = []
                    out[eid] = new
            continue    # その時点では「より新しい記録」が無い=印も順位も付けない
        d = len(newer) - len(kept)
        new = dict(info)
        new["newer"] = kept
        m = re.match(r"^(\d+)/(\d+)$", str(info.get("rank") or ""))
        if m:
            new["rank"] = "%d/%d" % (max(1, int(m.group(1)) - d), max(1, int(m.group(2)) - d))
        out[eid] = new
    return out


def shudai_seisaku(rows: list[dict], ito: str, shudai: dict | None) -> list[dict]:
    """主題を意味として扱う ―― 第3の政策(2026-09-09 ネオ・くくる改修指示書 P0-A)。

    既に在る2つの政策と同じ形で、別の軸だけを当てる:
      ・jikan_seisaku … 「今」の問いで、**型ごとの一律の期限**を過ぎた記録を最下段へ
      ・yuukou_seisaku … **名指しの訂正**が在る記録に⛔、名指しに入っていない同族に⚠️
      ・shudai_seisaku … **同じ物について、より新しい記録が在る**物を「今」の問いで最下段へ ←ここ

    なぜ要るか(くくるの指摘・2026-09-08 実測):
      頭脳は 2973件中 2941件を unknown のまま持っていて、「今どれが正しいか」を一度も裁いていない。
      名指しの訂正が無い限り、古い主張はいつまでも印なしで上位に立つ。
      くくるの指示=「current-state質問では回答投入前にFact裁定を必須化。古い候補は履歴として残すが
      current回答の既定候補から退役」。

    世界の答え(書く前に読んだ):
      MemStrata(arXiv:2606.26511)… 古い事実と今の事実は埋め込みが近すぎて、類似度では見分けられない。
        deterministic supersession で退役させるしかない。素のRAGは15〜40%で古い事実を返す。
      Quipu(arXiv:2608.16813)… 「latest-only の規則集合で評価すると50件全部を誤報告」。
        だから**過去を問う検索では絶対に落とさない**(下の ito=="kako" の扱い)。

    王国の掟を守る所:
      ・**⛔は打たない。** これは名指しでも審議でもなく、機械が見つけた並びに過ぎない
        (2026-09-08「広げるのは機械・⛔を打つのは名指しと審議だけ」)。付けるのは⏮の印だけ。
      ・**消さない・過去を問う検索では動かさない。** 当時の記録こそ正解。
      ・落とすのは「今」を問う時だけで、しかも最下段へ回すだけ(段の中の順はそのまま)。
      ・主題が取れなかった記録は今までどおり(判定しない=安全側)。
    """
    if not rows or not shudai:
        return rows
    tsuketa = []
    for row in rows:
        eid = str(row.get("event_id") or row.get("id") or "")
        info = shudai.get(eid)
        if not info:
            tsuketa.append((0, row))
            continue
        row = dict(row)
        row["shudai_rank"] = info.get("rank", "")
        row["shudai_newer"] = list(info.get("newer") or [])[:5]
        row["shudai_keys"] = list(info.get("keys") or [])[:5]
        row["shudai_shirushi"] = _shudai_shirushi_with_disputed(info)
        if info.get("disputed"):
            row["shudai_disputed"] = True
            row["shudai_disputed_reasons"] = list(info.get("disputed_reasons") or [])[:3]
        # 落とすのは「今」を問う時かつ「より新しい記録がある」時だけ(disputed単独では順位を動かさない)。
        # さらに(2026-09-09 ai-9598)「触れた物の名前だけで束ねられた記録」も動かさない。
        # 同じファイルに触った・同じポートに触れた、は「同じ物についての主張」ではない。
        has_newer = bool(info.get("newer"))
        kiku = has_newer and seisaku_no_kiku_kagi(info.get("keys"))
        if has_newer and not kiku:
            row["shudai_ugokasazu"] = "触れた物の名前だけの主題(印だけ・順位は動かさない)"
        tsuketa.append((1 if (ito == "ima" and kiku) else 0, row))
    tsuketa.sort(key=lambda pair: pair[0])      # 安定ソート=段の中は元の順のまま
    return [row for _, row in tsuketa]


def jikan_seisaku(rows: list[dict], ito: str, now: datetime | None = None) -> list[dict]:
    """時間を意味として扱う(2026-08-29 Code・くくるの提案書 P0-2)。

    リランカーは質問と文書の**関連度**しか見ない。時間もtrustも見ない。
    だから「今なにしてる？」に49日前の指示書が3位で立てた(2026-08-28 実測)。
    ここは関連度に手を触れず、**時間の妥当性という別の軸**だけを当てる。

    ・問いが「今」を向いている時 … 有効期限を過ぎた記録を最下段へ落とす(消さない)。
      期限は事実の型ごと(IMA_KIGEN_NICHI)。出典は円卓の正本2024件の実測。
    ・問いが「過去の一点」を向いている時 … 何も落とさない。当時の記録こそ正解。
    ・時間を問うていない時 … 何もしない。

    落とすだけで、順序は安定(同じ段の中ではリランカーの順を保つ)。
    印(ima_kigen_gire)を残すので、読む側が「なぜ下なのか」を見られる。
    """
    if not rows:
        return rows
    if ito != "ima":
        return rows
    due = []
    for row in rows:
        gire = kigen_gire(row, now)
        if gire:
            row = dict(row)
            row["ima_kigen_gire"] = True
        due.append((1 if gire else 0, row))
    due.sort(key=lambda pair: pair[0])          # 安定ソート = 段の中は元の順のまま
    return [row for _, row in due]


# 政策(yuukou_seisaku / shudai_seisaku / jikan_seisaku)が行へ付ける欄。
#
# ⚠️なぜ定数にしたか(2026-09-12 ネオ ai-e8ea・同じ穴の3度目):
#   起動束(context_pack)は keep で欄を絞る。**政策が新しい欄を足すたびに keep へ足し忘れ、
#   検索の口には出るのに要約(起動束)からは黙って消える。**
#   1度目=2026-09-07 に訂正の印(shirushi)で踏み、2度目=2026-09-09 に主題の印(⏮)で踏んだ
#   (実測: 本番の訂正対象18件を同じ問いで両方の口へ投げ、4件で search にだけ ⏮ が出た)。
#   気をつけるのをやめて、**付ける側と残す側を同じ一覧から引く**形にした。
#   政策に欄を足す時は、ここに足すだけでよい。
SEISAKU_NO_RAN = (
    # 訂正(名指し)
    "memory_state", "superseded_by_event_id", "superseded_at", "superseded_until",
    "supersession_scope", "shirushi", "superseded_kusari_owari", "superseded_youten",
    # 同族(名指しに入っていない同じ主張)
    "utagai_by_event_id", "utagai_at", "utagai_shimon",
    # 主題(同じ物について、より新しい記録がある)
    "shudai_shirushi", "shudai_rank", "shudai_newer", "shudai_ugokasazu", "shudai_disputed", "shudai_disputed_reasons",
    # 過去を問う問いでの訂正(順位は動かさない・印だけ)
    "kako_teisei_ari", "kako_teisei_by_event_id", "kako_teisei_at", "kako_teisei_scope",
)


def youten_no_shirushi(row: dict) -> str:
    """印に足す「では正しくは何か」の1行。訂正の本体が結果に無くても読めるようにする。"""
    youten = str(row.get("superseded_youten") or "").strip()
    return (" 訂正の要点:%s" % youten) if youten else ""


def kusari_no_shirushi(row: dict) -> str:
    """訂正が連なっている時に印へ足す1行。指し先の訂正も退けられている事を隠さない。"""
    owari = str(row.get("superseded_kusari_owari") or "")
    if not owari:
        return ""
    return " ⚠この訂正自身も後に訂正されている→今の答えは %s を見よ" % owari


def teisei_shirushi(row: dict) -> str:
    """訂正の状態を人(AI)が読める1行にする。欄が無い物には何も付けない(応答を太らせない)。

    replaced … 訂正された旧記録。「今の答えに使うな」と、訂正した側のevent_id・日付・範囲を添える
    current  … 訂正の記録そのもの(現在有効)
    inactive … 訂正の記録だが有効期間の外
    """
    st = str(row.get("memory_state") or "")
    if st == "replaced":
        made = str(row.get("superseded_until") or "")[:10]
        return ("⛔訂正済みの旧記録(今の答えに使うな)→訂正=%s %s%s 範囲:%s%s" % (
            str(row.get("superseded_by_event_id") or "?"),
            str(row.get("superseded_at") or "")[:10],
            ("〜" + made) if made else "〜(期限なし)",
            str(row.get("supersession_scope") or "")[:80],
            kusari_no_shirushi(row) + youten_no_shirushi(row)))
    if st == "suspect":
        return ("⚠️同じ主張への訂正がある(この投稿は名指しに入っていない・未審議)→訂正=%s %s 指紋:%s" % (
            str(row.get("utagai_by_event_id") or "?"),
            str(row.get("utagai_at") or "")[:10],
            "&".join(str(word) for word in (row.get("utagai_shimon") or []))[:60]))
    if st == "current":
        return "✅訂正の記録(現在有効)"
    if st == "inactive":
        return "⏸訂正の記録(有効期間の外)"
    # ★2026-09-21 ネオ ai-6162: 根拠にした記録が訂正された紙(提案書・報告書・話題のMarkdown)。
    #   **⛔は打たない** ―― この紙が打ち消されたのではなく、根拠の側が訂正された。
    moto = row.get("moto_ga_teisei_sareta") or []
    if moto:
        m = moto[0]
        return ("⚠️この記録が**根拠に挙げている記録**に訂正がある(この紙自体は打ち消されていない)"
                "→訂正=%s %s 根拠=%s%s 範囲:%s" % (
                    str(m.get("correcting_event_id") or "?"),
                    str(m.get("valid_from") or "")[:10],
                    str(m.get("moto_no_event_id") or "?"),
                    ("・ほか%d件" % (len(moto) - 1)) if len(moto) > 1 else "",
                    str(m.get("scope") or "")[:60]))
    return ""


def mikakutei_shirushi(row: dict, now: datetime | None = None) -> dict:
    """訂正の窓は在るが、この問いの語に当たらなかった記録へ「訂正が在る」とだけ告げる。

    なぜ要るか(2026-09-14 ネオ ai-dbb1・実測):
      correction_matches() は台帳の match_terms が問いに含まれる時だけ訂正を当てる。
      だが match_terms は「訂正が**当たるか**(適用範囲)」を決める物で、
      「訂正が**在るか**(告知)」を決める物ではない。ここが混ざっていたため、
      語が合わない問いでは**訂正された記録が完全に無印で上位に出ていた**。
      実測(reports/shissou_deguchi_baseline_20260914.json): 古い記録が上位に出た10組のうち2組が無印。
      中立な問い(「〜は今どうなっている」)では2組中2組が無印。
      実物=01787212724340417676-0288aa94 は match_terms に「札を下ろす」等を持つが、
      問い「チムからの作業指示 は今どうなっている」にその語が無いので無印で3位に出ていた。
      (訂正した側には「✅訂正の記録(現在有効)」が付くのに、訂正された側には何も付かない)

    やらない事(教訓9・波及を追った結果):
      ・memory_state は付けない ―― 付けると下流4箇所が動く(kako_no_shirushi の説明を参照)
      ・**順位は動かさない**
      ・既に印がある行は触らない ―― 情報を足すだけ
    """
    if row.get("shirushi"):
        return row
    aru = [window for window in (row.get("supersession_windows") or [])
           if isinstance(window, dict) and _window_active(
               str(window.get("valid_from") or ""),
               str(window.get("valid_until") or ""), now)]
    if not aru:
        return row
    row = dict(row)
    row["shirushi"] = "⚠️この記録には後の訂正がある(この問いに当たるかは未判定)"
    return row


def kako_no_shirushi(row: dict, now: datetime | None = None, query: str = "") -> dict:
    """過去を問う問い用の印。**順位も memory_state も動かさず**、「後に訂正された」だけを告げる。

    なぜ要るか(2026-09-12 ネオ ai-e8ea・実測):
      yuukou_seisaku は ito=="kako" で何もせずに返していた。順位を動かさないのは正しい
      (当時の記録こそ正解・Quipu arXiv:2608.16813「latest-only の規則集合で評価すると
      50件全部を誤報告」)。だが**印まで消していた**ので、読む側は訂正済みと知らずに読んでいた。
      実測: 訂正2件が載っている記録が検索1位・supersession_windows は行に在るのに
      memory_state も印も None。実際の問い10,940件の25.9%(2,837件)がこの道を通る
      (日付を書くだけで KAKO_RE に当たる)。

    なぜ memory_state を付けないか(教訓9・波及を追った結果):
      "replaced" を付けると下流4箇所が動く ―― チムの材料除外(entaku_nou.py:271)・
      王の指定席(_ou_no_kiroku)・markdown・事実層との突き合わせ。
      チム側の「過去問か」の判定は KAKO_NO_KOTOBA で**頭脳の KAKO_RE とは別定義**なので、
      頭脳が kako・チムが非過去問と見る問いでは**当時の記録が答えから消える**。
      ここは情報を足すだけに留める。状態を変えるなら定義を1本にしてからにする。
    """
    active = [window for window in (row.get("supersession_windows") or [])
              if isinstance(window, dict) and _window_active(
                  str(window.get("valid_from") or ""),
                  str(window.get("valid_until") or ""), now)
              and correction_matches(window, query)]
    if not active:
        # この行自身が訂正の記録なら、それも言う。
        # 2026-09-12 実測: 訂正された側にだけ印を付けて、訂正した側(=新しい主張)には
        # 何も付けていなかった。読む側は「どちらが後から出た話か」を印から読めない。
        if row.get("correction_id") and correction_current(row, now):
            row = dict(row)
            row["shirushi"] = "✅これは訂正の記録(今も有効)"
        # 語が当たらなくても「訂正が在る」事だけは告げる(2026-09-14・順位は動かさない)
        return mikakutei_shirushi(row, now)
    newest = max(active, key=lambda window: (
        str(window.get("valid_from") or ""),
        str(window.get("correction_id") or "")))
    row = dict(row)
    row["kako_teisei_ari"] = True
    row["kako_teisei_by_event_id"] = str(newest.get("correcting_event_id") or "")
    row["kako_teisei_at"] = str(newest.get("valid_from") or "")
    row["kako_teisei_scope"] = str(newest.get("scope") or "")
    row["superseded_kusari_owari"] = str(newest.get("kusari_owari") or "")
    row["superseded_youten"] = str(newest.get("youten") or "")
    row["shirushi"] = ("⏳当時の記録(この問いの答えとしては有効)。その後 %s に訂正された"
                       "→訂正=%s 範囲:%s%s" % (
                           str(newest.get("valid_from") or "")[:10],
                           str(newest.get("correcting_event_id") or "?"),
                           str(newest.get("scope") or "")[:60],
                           kusari_no_shirushi(row) + youten_no_shirushi(row)))
    return row


def yuukou_seisaku(rows: list[dict], ito: str, now: datetime | None = None,
                   query: str = "") -> list[dict]:
    """訂正の有効期間を検索順位へ反映する。

    旧記録は削除せず、現在または時間指定なしの問いでは active な置換対象だけを
    最下段へ落とす。過去を問う検索では当時の記録をそのまま返す。
    """
    if not rows:
        return rows
    if ito == "kako":
        # 過去を問う問いは**並べ替えない**(当時の記録こそ正解)。印だけ付ける。
        # 2026-09-12 まで、ここは印も付けずに返していた(穴の記録=kako_no_shirushi の説明)。
        return [kako_no_shirushi(row, now, query) for row in rows]
    ranked = []
    for row in rows:
        active = [window for window in (row.get("supersession_windows") or [])
                  if isinstance(window, dict) and _window_active(
                      str(window.get("valid_from") or ""),
                      str(window.get("valid_until") or ""), now)
                  and correction_matches(window, query)]
        if active:
            # 同一対象に訂正鎖がある場合は、最も新しく始まった窓を表示上の代表にする。
            newest = max(active, key=lambda window: (
                str(window.get("valid_from") or ""),
                str(window.get("correction_id") or ""),
            ))
            row = dict(row)
            row["memory_state"] = "replaced"
            row["superseded_by_event_id"] = str(newest.get("correcting_event_id") or "")
            row["superseded_at"] = str(newest.get("valid_from") or "")
            row["superseded_until"] = str(newest.get("valid_until") or "")
            row["supersession_scope"] = str(newest.get("scope") or "")
            row["superseded_kusari_owari"] = str(newest.get("kusari_owari") or "")
            row["superseded_youten"] = str(newest.get("youten") or "")
            # 読む側が一目で分かる印(2026-09-07 ネオ・願い negai-20260907-125323)。
            # 判定はここ1箇所、出口(検索/起動束/markdown)はこの欄を通すだけ(教訓9「入口で1回」)。
            row["shirushi"] = teisei_shirushi(row)
        elif row.get("correction_id"):
            row = dict(row)
            row["memory_state"] = "current" if correction_current(row, now) else "inactive"
            row["shirushi"] = teisei_shirushi(row)
        else:
            # 同族(名指しに入っていないが同じ主張を書いた投稿)。⚠️を付けるだけで
            # **順位は動かさない** ―― 機械の見立てで王の記録を沈めない(2026-09-08)。
            kin = [window for window in (row.get("kin_windows") or [])
                   if isinstance(window, dict) and _window_active(
                       str(window.get("valid_from") or ""),
                       str(window.get("valid_until") or ""), now)
                   and correction_matches(window, query)]
            if kin:
                newest = max(kin, key=lambda window: (
                    str(window.get("valid_from") or ""),
                    str(window.get("correction_id") or "")))
                row = dict(row)
                row["memory_state"] = "suspect"
                row["utagai_by_event_id"] = str(newest.get("correcting_event_id") or "")
                row["utagai_at"] = str(newest.get("valid_from") or "")
                row["utagai_shimon"] = list(newest.get("shimon") or [])
                row["shirushi"] = teisei_shirushi(row)
        # 語が当たらず active が空の行にだけ「訂正が在る」事を告げる(2026-09-14)。
        # ⚠️active が立つ行へ被せると、teisei_shirushi を潰した時に印が残って
        #   「門番の門番」(test_wazato_kowasu_to_ochiru)が素通りする。実際に1度そう書いて落ちた。
        # 判定はここ1箇所。順位(第1要素)には一切効かせない。
        if not active:
            row = mikakutei_shirushi(row, now)
        # ★2026-09-21 ネオ ai-6162: 根拠にした記録が訂正された紙(提案書・報告書・話題)。
        #   ⚠️**問いの語に関わらず必ず告げる** ―― これは「訂正が**当たるか**(適用範囲)」ではなく
        #   「訂正が**在るか**(告知)」。この2つを混ぜた穴は2026-09-14に既に踏んでいる
        #   (mikakutei_shirushi の docstring)。
        #   ★既に印が在る行は上書きしない。上書きすると teisei_shirushi を潰しても印が残り、
        #   「門番の門番」が素通りする(上のコメントと同じ罠)。
        if not str(row.get("shirushi") or "") and row.get("moto_ga_teisei_sareta"):
            row = dict(row)
            row["shirushi"] = teisei_shirushi(row)
        ranked.append((1 if active else 0, row))
    ranked.sort(key=lambda pair: pair[0])       # 安定ソート: 関連度の段内順を守る
    return [row for _, row in ranked]


def _ou_no_kiroku(row: dict) -> bool:
    """王の裁定そのものの記録か。proof=王合格、または王の判定台帳のファイル。"""
    if str(row.get("memory_state") or "") == "replaced":
        return False
    if str(row.get("proof") or "") == "王合格":
        return True
    return "王の判定/" in str(row.get("path") or "")



def _kanten_kouho_ichii(kanten: str, kouho: list[dict], nozoku=()) -> dict | None:
    """候補(50件)の中から、この観点にいちばん当たっている1件を選ぶ。決定論・BM25のIDFだけ。"""
    terms = query_terms(kanten)
    if not terms or not kouho:
        return None
    texts = [normalize(" ".join(str(r.get(k) or "") for k in
                                ("text", "summary", "title", "topic", "path")))
             for r in kouho]
    total = max(1, len(kouho))
    idf = {}
    for term in terms:
        df = sum(1 for x in texts if term in x)
        idf[term] = math.log((total - df + 0.5) / (df + 0.5) + 1.0)
    best, saikou = None, 0.0
    for row, text in zip(kouho, texts):
        if not text or item_key(row) in nozoku:
            continue
        score = sum(idf[t] * text.count(t) for t in terms if t in text)
        if score > saikou:
            best, saikou = row, score
    return best


# 観点1つあたり確保する席の数。★2026-09-14 実測で決めた（凍結でなく本番の索引・11問）:
#   3件=全部当たる0.273 / **5件=0.364** / 7件=0.364（5で頭打ち）。一部以上はどれも0.818。
#   出典 reports/Code_kanten_seki_20260914/（前=0.182/0.727）。数字を変えたら測り直すこと。
KANTEN_SEKI_KAZU = int(os.environ.get("KANTEN_SEKI_KAZU", "5"))
# 観点に「属する」と見なす順位の深さ。ここに居る行は押し出さない(無所属の行だけ空ける)。
# ★2026-09-14 実測(本番の索引・11問・reports/Code_kanten_seki_20260914/):
#   深さ 2=0.545 / **3=0.636 / 5=0.636 / 7=0.636** / 10=0.545 / 20=0.545 / 50=0.455
#   深すぎると「ほとんどの行が属する」判定になり**空き席が無くなって席が座れない**
#   (v2q330 で席が1つしか座っていなかった)。3〜7が平らなので真ん中の5を採る。
#   一部以上はどの深さでも 0.909 で変わらない。数字を変えたら測り直すこと。
KANTEN_SEKI_ZOKU = int(os.environ.get("KANTEN_SEKI_ZOKU", "5"))


def _kanten_yuugou_juni(kanten: str, items: list, hays: list, avgdl: float,
                        root=None, moji_made: int = 200, imi_made: int = 50,
                        imi_atatta: set | None = None) -> list:
    """1つの観点で「字面BM25」と「意味(bge-m3)」を別々に検索し、RRFで融合した鍵の順位を返す。

    ★2026-09-14 実測（サカナAI fugu が197往復で残した測定器 /tmp/select.py の結果・14観点）:
        観点の代表を **字面BM25の一位だけ**で選ぶと正解を当てたのは **4/14**。
        字面と意味を RRF で融合した一位なら **5/14**。
        さらに大きいのは「正解は融合順の 1〜21位に散らばっている」事で、
        **一位だけ見ていたから落ちていた**（例 v2q333 B=5位 / v2q340 B=8位 / v2q328 B=15位）。
    RRF(k=60) は世界の定石（Cormack et al. 2009 の Reciprocal Rank Fusion）で、
    王国の `imi_wo_mazeru` も同じ融合を使っている。ここで新しい式は発明していない。

    意味の車線が使えない時は字面だけの順位を返す（fail-open。検索を止めない）。
    """
    terms = query_terms(kanten)
    if not terms or not hays:
        return []
    total = max(1, len(items))
    idx_cache = _hay_index_for(hays, avgdl)
    kscores = _bm25_taat_scores(hays, avgdl, terms, total, idx_cache)
    n_items = len(items)
    moji = [(sc, idx) for idx, sc in kscores.items() if idx < n_items and sc > 0]
    moji.sort(key=lambda x: (-x[0], x[1]))
    moji = moji[:moji_made]
    ten: dict = {}
    for juni, (_score, index) in enumerate(moji, 1):
        ten[item_key(items[index])] = ten.get(item_key(items[index]), 0.0) + 1.0 / (60 + juni)
    if root is not None:
        try:
            import roundtable_imi
            jotai, chikai = roundtable_imi.sagasu(root, kanten, limit=imi_made)
            if jotai in ("ok", "fuicchi"):
                for juni, (kagi, _chika) in enumerate(chikai, 1):
                    ten[kagi] = ten.get(kagi, 0.0) + 1.0 / (60 + juni)
                    if imi_atatta is not None:
                        imi_atatta.add(kagi)      # この観点では意味の車線で当たった証拠
        except Exception:
            pass            # fail-open: 意味の車線が落ちても字面だけで続ける
    return [kagi for kagi, _t in sorted(ten.items(), key=lambda kv: -kv[1])]


# 観点ごとの順位をリランカーへ通す時、上位何件を渡すか。
# ★2026-09-14 実測(reports/Code_kanten_seki_20260914/)：30件渡すと
#   v2q330 の正解が 21位→3位 / 17位→4位 まで上がり、席5件に両方入るようになった。
#   渡す数を増やすほどリランカーが重くなるので、効いた最小の30で止めている。
KANTEN_RERANK_KOUHO = int(os.environ.get("KANTEN_RERANK_KOUHO", "30"))


def _kanten_ni_narabe_naosu(kanten: str, juni: list, items: list) -> list:
    """観点の順位の上位を、交差符号化リランカーで並べ直す。

    ★なぜ要るか（2026-09-14 実測）: 字面＋意味のRRF融合だけでは、正解が**融合順の深い所**
      （15〜21位）に居る問いがあり、席5件に届かなかった。リランカーは問いと文を突き合わせるので、
      同じ候補のまま順位だけを正した（21位→3位・17位→4位）。

    リランカーが落ちていれば `narabe_naosu` が元の順で返す＝**融合順のまま**。検索は止めない。
    並べ直すのは上位 KANTEN_RERANK_KOUHO 件だけで、残りは後ろにそのまま繋ぐ（順位を失わない）。
    """
    if not juni or len(juni) < 2:
        return juni
    ue = juni[:KANTEN_RERANK_KOUHO]
    shita = juni[KANTEN_RERANK_KOUHO:]
    try:
        byKey = {item_key(i): i for i in items}
        gyou = [shiage(byKey[k], 0.0) for k in ue if k in byKey]
        if len(gyou) < 2:
            return juni
        naranda = [item_key(r) for r in narabe_naosu(kanten, gyou, len(gyou))]
        nokori = [k for k in ue if k not in set(naranda)]
        return naranda + nokori + shita
    except Exception:
        return juni          # fail-open。並べ直せなくても融合順で進む


def kanten_no_seki(ranked: list[dict], kouho: list[dict], limit: int,
                   items: list, hays: list, avgdl: float, query: str,
                   root=None, diagnostics: dict | None = None,
                   ito: str | None = None,
                   as_of: datetime | None = None,
                   cpu_only: bool = False) -> list[dict]:
    """観点の指定席(2026-09-13 ネオ ai-9749 / 2026-09-14 に選び方を入れ替えた)。

    王が「AとB、両方どうなった？」と2つの事を一度に聞いた時、A側・B側それぞれの代表が
    上位に1つも居なければ、**末尾の席をそれに差し替える**。

    ★なぜ「足す」ではなく「守る」のか（2026-09-13の実測で確定）:
      観点ごとにBM25の一位を候補へ足す実装を作ったが、**追加できた件数は0**だった。
      観点の一位は元の検索でも既に候補に入っていたからだ。
      問題は順位で、**リランカーが上位から蹴り落とし、その中に正解が入っていた**。

    ★2026-09-14 に直した2つ（サカナAI fugu の197往復の掘り出し）:
      ① 代表の選び方を **字面BM25の一位** から **字面＋意味のRRF融合** へ
         （14観点で 4/14 → 5/14。`_kanten_yuugou_juni` の docstring に実測）
      ② 席を **1件** から **KANTEN_SEKI_KAZU 件** へ
         （正解は融合順の1〜21位に散らばる。1位だけ見ていたから落ちていた）

    作法は `ou_no_seki`(王の指定席)に合わせた:
      1位は奪わない / 他の席(王・道具・主題)が座った行は奪わない /
      割れない問いには何もしない（今までの検索を1ミリも変えない）。

    ★席に座る行にも**訂正・期限の政策を掛ける**（バグ探し retr-2・2026-10-31）:
      席の行は索引の生の行(items)から shiage で作る。ここは search_snapshot の
      yuukou_seisaku / jikan_seisaku の**後**に呼ばれるので、掛け直さないと、訂正で打ち消された
      旧記録が打消しの印も memory_state も無いまま座る（王・道具・主題の席は政策を通した
      kouho から取るので無傷だった）。ito / as_of は search_snapshot と同じ値を渡す。
      ito を渡されなければ jikan_ito(query) で求める（search_snapshot と同じ導き方）。
    """
    wareta = kanten_ni_wakeru(query)
    if not ranked or limit < 2 or not wareta:
        return ranked
    if ito is None:
        ito = jikan_ito(query)
    head = list(ranked[:limit])
    totta = {item_key(r) for r in head}
    tsukatta: dict = {}
    imi_atatta: set = set()
    if cpu_only or _is_cpu_agent_active() or (isinstance(diagnostics, dict) and diagnostics.get("brain_mode") == "cpu_agent"):
        juni_hyou = {k: _kanten_yuugou_juni(k, items, hays, avgdl, None, imi_atatta=imi_atatta)
                     for k in wareta}
    else:
        juni_hyou = {k: _kanten_ni_narabe_naosu(
                            k, _kanten_yuugou_juni(k, items, hays, avgdl, root,
                                                   imi_atatta=imi_atatta), items)
                     for k in wareta}
    # ★どの観点にも属さない行(無所属)だけを差し替える(2026-09-14)。
    #   実測: 末尾から無差別に押し出すと、元から上位に居た正解を消していた
    #   (席5件で 一部以上 0.727→0.636)。世界の定石 xQuAD は「押し出す」のではなく
    #   「どの側面もカバーされるように選び直す」。無所属が無ければ何もしない。
    zokusuru = set()
    for kagi_retsu in juni_hyou.values():
        zokusuru.update(kagi_retsu[:KANTEN_SEKI_ZOKU])
    akeru = [n for n in range(len(head) - 1, 0, -1)          # 1位は奪わない・末尾から
             if item_key(head[n]) not in zokusuru
             and not (head[n].get("ou_no_seki") or head[n].get("dougu_no_seki")
                      or head[n].get("shudai_no_seki"))]
    for kanten in wareta:
        juni = juni_hyou.get(kanten) or []
        nokori = KANTEN_SEKI_KAZU
        for kagi in juni:
            if nokori <= 0:
                break
            if kagi in totta:
                # 既に上位に居る＝この観点は守れている。席は使わない
                nokori -= 1
                tsukatta.setdefault(kanten, []).append(kagi)
                continue
            if not akeru:
                break
            ichi = akeru.pop(0)
            motome = None
            for it in items:
                if item_key(it) == kagi:
                    motome = it
                    break
            if motome is None:
                continue
            osi = shiage(motome, 0.0)
            osi = yuukou_seisaku(jikan_seisaku([osi], ito, as_of),
                                 ito, as_of, query=query)[0]
            osi["kanten_seki"] = kanten
            if kagi in imi_atatta:
                # ★観点ごとに引けば意味の車線で当たった＝「意味で近い物がゼロ」ではない。
                #   棄権の門(public_abstention)がこの印を見る。
                osi["kanten_imi"] = kanten
            totta.discard(item_key(head[ichi]))
            head[ichi] = osi
            totta.add(kagi)
            tsukatta.setdefault(kanten, []).append(kagi)
            nokori -= 1
        if not akeru:
            break
    if diagnostics is not None:
        diagnostics["kanten_seki"] = {"kanten": list(wareta),
                                      "seki_kazu": KANTEN_SEKI_KAZU,
                                      "tsukatta": {k: v[:KANTEN_SEKI_KAZU]
                                                   for k, v in tsukatta.items()}}
    return head + list(ranked[limit:])


# 王の裁定を聞いている問いか（2026-09-21 ネオ ai-8b7c）。
#   ⚠️「王国」の王を拾わない。golden 85問で q007「c2paって王国でも使えるのか？」が
#     これで誤って当たった（実測）。王国はこの脳のどこにでも出る語なので、必ず外す。
OU_ITO_RE = re.compile(r"王(?!国)|陛下|裁定|王命|決めた|決定|命じ|可否|お達し|お墨付き")


def ou_ito(query: str) -> bool:
    """問いが「王が何を決めたか」を向いているか。"""
    return bool(OU_ITO_RE.search(str(query or "")))


_RECORD_ID_TOKEN = re.compile(r"(?<!\w)[0-9]{20}-[0-9a-f]{8}:[0-9a-f]{16}(?!\w)")


def supersedes_no_seki(ranked: list[dict], items: list[dict], limit: int,
                       query: str, ito: str, as_of: datetime | None,
                       participant: str, scope: str) -> list[dict]:
    """旧 record_id 検索時に、それを訂正した新記録(supersedes_record_id一致)を先頭席へ置く。

    クエリに record_id トークンが無い通常の問い・過去指定(ito=="kako")では即座に素通しし、
    既存の検索順位へ一切影響を与えない。
    """
    if not query or ito == "kako" or limit < 1:
        return ranked
    q_rids = set(_RECORD_ID_TOKEN.findall(query))
    if not q_rids:
        return ranked
    matched: list[dict] = []
    for it in items:
        s_rid = str(it.get("supersedes_record_id") or "")
        s_list = [str(x) for x in (it.get("superseded_record_ids") or []) if str(x)]
        if s_rid not in q_rids and not any(r in q_rids for r in s_list):
            continue
        if not _participant_matches(it, participant, scope):
            continue
        if it.get("correction_id") and not correction_current(it, as_of):
            continue
        matched.append(it)
    if not matched:
        return ranked
    matched.sort(key=lambda r: str(r.get("timestamp") or ""), reverse=True)
    promoted: list[dict] = []
    promoted_keys: set[str] = set()
    for it in matched:
        row = shiage(it, 100.0)
        row = yuukou_seisaku([row], ito, as_of, query=query)[0]
        row["supersedes_no_seki"] = True
        k = str(row.get("event_id") or row.get("record_id") or row.get("id") or "")
        if k and k in promoted_keys:
            continue
        if k:
            promoted_keys.add(k)
        promoted.append(row)
    rest = [
        r for r in ranked
        if str(r.get("event_id") or r.get("record_id") or r.get("id") or "") not in promoted_keys
    ]
    return (promoted + rest)[:limit]


def ou_no_seki(ranked: list[dict], kouho: list[dict], limit: int,
               query: str = "") -> list[dict]:
    """王の判定の指定席(2026-08-24 ネオ)。

    実測: リランカーはtrustもproofも見ないため、雑談1件で王の判定が1位→5位へ
    押し出された(試験場)。候補(文字+意味で既に当たっている50件)の中に王の裁定の
    記録が居るのに上位から漏れた時だけ、最後の1席をそれに差し替える。
    1位は奪わない(押し出す害を最後の1席に限定する)。候補に居ない問いには何もしない。"""
    if not ranked or limit < 2:
        return ranked
    # ★意図の関(2026-09-21 ネオ ai-8b7c)。**王を聞いていない問いでは席を使わない。**
    #
    # 実測(golden.jsonl 85問・本番の深さ3・2026-09-21):
    #   席は **85問中53問(62%)で働き**、head[:limit-1] で必ず3件目を捨てていた。
    #   その代償 = **R@3 0.700(席なし) → 0.650(席あり)**。3問(q014/q046/q052)で正解が消えた。
    #   一方で席を外すと王の裁定が上位3件に出る率は **1.000 → 0.333** へ落ちる。
    #   ＝ 席は効いているが、**効かせる場所を選んでいなかった**。
    #
    # 潰した筋(2026-09-21 実測・報告書に残す):
    #   世界の作法(AuthGR arXiv:2604.13468)の sidecar=加点で置き換える案は**王国では悪化した**。
    #   王の裁定が1.000に戻る強さ(加点6.0)まで上げると **R@3 0.633・R@1 0.433** で席より悪い。
    #   加点は問いを選ばず全部に効くため。AuthGR が重みを**学習**しているのはそのため。
    #   ＝ 単一の定数の加点は採らない。**条件を絞る方(この関)を採る。**
    #
    # 王国には同じ型が既に在る: dougu_no_seki は dougu_ito(query) で問いを選んでいる(2026-09-05)。
    # 世界も同じ事を言う: 「何が権威かは問いの型による」(Authority in RAG Systems)。
    if query and not ou_ito(query):
        return ranked
    head = ranked[:limit]
    if any(_ou_no_kiroku(r) for r in head):
        return ranked
    for row in kouho:
        if _ou_no_kiroku(row):
            osimark = dict(row)
            osimark["ou_no_seki"] = True
            return head[:limit - 1] + [osimark]
    return ranked


# 2026-09-05 修理(バグ探し高#4): 「道具は何」「やり方は」型の問い。
#   実測: 「動画の仕上げ磨きの道具は何だっけ」で1位2位が Code 自身の修理報告(note・本文に王の問いを
#   丸ごと引用・rerank 0.99/0.95)、本当の答えの道具台帳 migaki は3位(rerank 0.136)。既定 limit=3 で
#   3枠中2枠が報告に潰れていた。リランカーは「問いの文をそのまま含む文書」を最も関連ありと見る。
#   『バグ報告を書くほど、そのバグの対象を検索で見つけにくくなる』自家中毒。
#   世界の解: 問いの意図で経路/重みを分ける(query intent → doc type)。くくる提案2026-09-05 D-③も同じ。
DOUGU_RE = re.compile(r"道具|ツール|スクリプト|コマンド|やり方|手順|使い方|どうやって|どうやる|方法|何を使")


def dougu_ito(query: str) -> bool:
    """問いが「道具・やり方」を向いているか。"""
    return bool(DOUGU_RE.search(str(query or "")))


def dougu_no_seki(ranked: list[dict], kouho: list[dict], limit: int, query: str) -> list[dict]:
    """道具の指定席(2026-09-05 ネオ・バグ探し高#4)。

    問いが道具・やり方を向いていて、上位 limit 席に procedural(手順・道具台帳)が1つも無く、
    候補(文字+意味で当たっている50件)の中に居る時だけ、その最良の1件を **1席目** に置く。
    王の判定の指定席(ou_no_seki)が最後の1席を守るのと対で、こちらは先頭を守る。
    理由: 「道具は何」と聞く王が欲しいのは道具そのもので、それを直した報告ではない。
    候補に居ない問い・道具型でない問いには何もしない(順位には一切触れない)。"""
    if not ranked or limit < 1 or not dougu_ito(query):
        return ranked[:limit]
    head = ranked[:limit]
    if str(head[0].get("memory_type") or "") == "procedural":
        return head
    # 1席目が手順でない時だけ動かす。上位に手順が「在る」だけでは足りない——
    # 実測(2026-09-05): [報告,報告,migaki] は素通りし、最後の席を王の裁定(ou_no_seki)が
    # 取って migaki が消えた。先頭を守るのがこの席の仕事。
    top_yugo = head[0].get("rr_yugo")
    for pool in (ranked, kouho):        # リランカーの順 → 文字+意味の順
        for row in pool:
            if str(row.get("memory_type") or "") == "procedural":
                seki = dict(row)
                seki["dougu_no_seki"] = True
                rid = str(row.get("id") or row.get("record_id") or "")
                # 1位と手順候補の両方にリランカー融合点が在り、手順候補が極端に低得点または
                # 1位と大差がある時は、正解の1位を強奪せず2席目へ保護する。
                if top_yugo is not None and row.get("rr_yugo") is not None:
                    rr = float(row.get("rerank") or 0.0)
                    if rr < 0.15 or (float(top_yugo) - float(row.get("rr_yugo"))) > 1.5:
                        if limit >= 2 and not any(str(r.get("memory_type") or "") == "procedural" for r in head):
                            rest2 = [r for r in head[1:]
                                     if r is not row and (not rid or str(r.get("id") or r.get("record_id") or "") != rid)]
                            return ([head[0], seki] + rest2)[:limit]
                        return head
                rest = [r for r in head
                        if r is not row and (not rid or str(r.get("id") or r.get("record_id") or "") != rid)]
                return ([seki] + rest)[:limit]
    return head


def _shudai_name_forms(value: str) -> set[str]:
    raw = str(value or "").strip().replace("\\", "/").rsplit("/", 1)[-1].lower()
    if not raw: return set()
    forms = {raw}
    if "." in raw: forms.add(raw.rsplit(".", 1)[0])
    return {x for x in forms if x}


def _shudai_evidence_row(row: dict) -> bool:
    """A hit marker alone is not evidence (Codex independent review 2026-09-12)."""
    return bool(row.get("id") or row.get("event_id") or row.get("record_id")) and any(
        str(row.get(key) or "").strip()
        for key in ("summary", "snippet", "title", "topic", "_rerank_bun"))


def _shudai_ni_fureteiru(row: dict, namae: list[str]) -> bool:
    """王の目に入る欄(要点・断片・題名・話題)に、問いが名指しした名前が出ているか。
    本文全文(_text_zenbun)は見ない ―― 末尾で一度触れただけの記録まで「思い出せた」に
    数えてしまい、席が永久に発火しなくなる。"""
    if row.get("_shudai_hit") and _shudai_evidence_row(row):
        return True
    t = " ".join(str(row.get(k) or "") for k in
                 ("summary", "snippet", "title", "topic", "_rerank_bun"))
    tl = t.lower().replace("\\", "/")
    return any(any(form in tl for form in _shudai_name_forms(n)) for n in namae)


def shudai_no_seki(ranked: list[dict], kouho: list[dict], limit: int,
                  kagi: list | None = None,
                  diagnostics: dict | None = None, query: str | None = None) -> list[dict]:
    """主題の指定席(2026-09-09 ネオ・王命1129の残り1件)。

    問いが名指しした主題(ファイル名・ポート・service名)の記録が候補には居るのに、
    上位 limit 席に1件も残らなかった時だけ、**指定席でない自由席の最下位1席**を
    その最良1件に譲る。1位は奪わず、自由席が2つ以上ある時しか動かない。
    主題キーが取れない問い(印が1件も無い)には何もしない。

    なぜ要るか(実測 2026-09-09): shudai_de_hirou が候補を先頭へ引き上げても、
    その後の交差符号化リランカーが並べ直して全部落としていた
    ―― 診断が {"jotai":"ugokashita","hikiageta":3} を返すのに、
    「joujuu_no_me.py は毎日何時に走るか」の最終8件に主題の記録が1件も無い。
    融合式の元順位の項は 0.75/(順位+1) しかなく、logit の差(数ポイント)に負ける。
    世界も同じ結論で、交差符号化は語の完全一致より広い意味一致を上へ置くため、
    第一段の完全一致が並べ直しで落ちる(BigDataBoutique / TianPan 2026)。
    定石は ①候補に両方入れる(=shudai_de_hirou・実装済) ②第一段の点を融合で守る
    ―― だが②は重みの実測が要る。王国には既に決定的な指定席の型が2つ在るので
    (ou_no_seki=最後の1席 / dougu_no_seki=先頭の1席)、重みを増やさずそちらへ寄せた。

    ⚠️ query は**受け取るだけで使っていない**(2026-09-09 Codex が壊し役として追加)。
    Codex の狙いは「誰が/担当者/作成者」型の属性質問で席を切る事だったが、
    ネオが本番データで実測したら3問とも席は不発火だった ―― 属性質問は上位に必ず
    その名前が出るので、**上の sudeni_iru で既に止まっている**。
    別の防御が要ると分かった時のために口だけ残してある。使うなら実測を添えてから。
    """
    if not ranked or limit < 2:
        return ranked
    namae = [str(k).split(":", 1)[-1] for k in (kagi or []) if str(k).split(":", 1)[-1]]
    if not namae:
        # 主題キーが取れない問い（王の商売・雑談など）には一切触らない
        if diagnostics is not None:
            diagnostics["shudai_seki"] = {"jotai": "shudai_nashi"}
        return ranked
    head = ranked[:limit]
    if any(_shudai_ni_fureteiru(r, namae) for r in head):
        # ⚠️ 「索引(seki_kagi)に載っているか」で見てはいけない。索引は主題に触れた記録の
        #    一部でしかなく、載っていない本物が上位に居ると、席がそれを押し出す。
        #    2026-09-09 実測: 「ai_news_koe.py は毎日何時に走るか」で、7位に居た
        #    正解(7:30を含む記録)を、索引に載っている別の記録が蹴り出した。
        #    上位に**その名前が出ている**なら、頭脳は既に思い出せている。触らない。
        if diagnostics is not None:
            diagnostics["shudai_seki"] = {"jotai": "sudeni_iru"}
        return ranked
    jiyuu = [i for i, r in enumerate(head)
             if not r.get("ou_no_seki") and not r.get("dougu_no_seki")]
    if len(jiyuu) < 2:
        # 自由席が1つ以下 = 席の取り合いで最後の1件まで特別席になる。害の方が大きいので何もしない
        if diagnostics is not None:
            diagnostics["shudai_seki"] = {"jotai": "yuzuranai", "why": "jiyuu_seki_lt_2"}
        return ranked
    for pool in (ranked, kouho):          # リランカーの順 → 文字+意味の順
        for row in pool:
            if row.get("_shudai_hit") and _shudai_evidence_row(row):
                seki = dict(row)
                seki["shudai_no_seki"] = True
                out = list(head)
                out[jiyuu[-1]] = seki
                if diagnostics is not None:
                    diagnostics["shudai_seki"] = {
                        "jotai": "yuzutta", "seki": jiyuu[-1] + 1,
                        "id": str(row.get("id") or "")[:40]}
                return out
    if diagnostics is not None:
        diagnostics["shudai_seki"] = {"jotai": "shudai_nashi"}
    return ranked




# ── 問いを「観点A」「観点B」に割る（2026-09-13 ネオ ai-9749 / 王命「お前が欲しい作業だけサカナにやらせろ」）
#
# サカナAI(fugu)が自分の前作(会話糸の1-hop辿り)を「効果ゼロ」と否定した上で出した設計 ――
#   「thread_id は"同じ会話の前後"を束ねる鍵であって、"問いに含まれる別々の話題"を束ねる鍵ではない。
#    必要なのは、問いを複数の観点へ分けて各観点を別々に検索し、最低1件ずつ確保してから統合する経路」
# その第一段。**決定論だけ。LLMもリランカーも呼ばない**
#   （リランカーが止まっていても検索が壊れないように。サカナ自身が負試験に挙げた条件）。
#
# 割れない問いには None を返す。無理に割ると、単一の記録で答えられる問いを2つに割って両方外す。
# 実測(2026-09-13): golden_fukusuu.jsonl の11問中8問を割り、割ってはいけない5問は1つも割らなかった。
#   割らなかった3問(v2q331/332/337)は接続詞では割れない型＝「前と後」「決定と理由」。分解では解けない。

KANTEN_AIZU = re.compile(r"両方|どっち|どちら|どう違|違った|違う|比べ|比較|それぞれ|一方|どうなった|どう変わ")
# ⚠️時間の前後(たあと/した後/あとの)は外した。2026-09-13にサカナAIの負試験が
#   「AIが勝手に直した後の、見逃してた失敗の確認、ちゃんと見てたよな？」を誤分解する事を捕まえた。
#   これは1つの話。代償として v2q335 が割れなくなる(8問→7問)が、誤分解で既存を壊すよりまし。
KANTEN_SETSUZOKU = ("けれど", "けど", "だが", "なのに",
                    "たあとの", "あとの")   # 2026-09-25 ネオ ai-2268: 下のコメントに在るのに実装から抜けていた。末尾＝既存の並びは動かさない
KANTEN_HASHI = "、。 \u3000？?のとがはをにでも"


def _kanten_hashi_wo_otosu(s: str) -> str:
    return s.strip().strip(KANTEN_HASHI).strip()


def _kanten_shippo_wo_otosu(q: str, aizu_i: int) -> str:
    """合図の句(「、両方どうなった？」など)を落として、観点が並んでいる所だけ残す。"""
    kiri = q.rfind("、", 0, aizu_i)
    if kiri > len(q) * 0.3:
        return q[:kiri]
    return _kanten_hashi_wo_otosu(q[:aizu_i])


def kanten_ni_wakeru(query: str):
    """複数観点の問いを (観点A, 観点B) に割る。割れない問いは None。

    ⚠️まだ検索経路からは呼んでいない（置いただけ）。繋ぐのは次の段で、
      「A/Bそれぞれで検索し、各観点から最低1件ずつ席を予約してから統合する」所まで作ってから。
    """
    q = (query or "").strip()
    if len(q) < 6:
        return None
    for k in KANTEN_SETSUZOKU:               # ①「Aけど、B」「Aあとの、B」
        i = q.find(k)
        if i <= 0:
            continue
        a, b = _kanten_hashi_wo_otosu(q[:i]), _kanten_hashi_wo_otosu(q[i + len(k):])
        if len(a) >= 5 and len(b) >= 5:   # 5字未満は「どうした？」のような中身の無い片割れ
            return a, b
    m = KANTEN_AIZU.search(q)                # ②「AとB、両方どうなった？」
    if not m:
        return None                          # 合図が無ければ割らない（誤分解しない）
    honbun = _kanten_shippo_wo_otosu(q, m.start())
    i = honbun.find("と")
    if i <= 0:
        return None
    a, b = _kanten_hashi_wo_otosu(honbun[:i]), _kanten_hashi_wo_otosu(honbun[i + 1:])
    if len(a) >= 5 and len(b) >= 5:
        return a, b
    return None


# 問いが「AとB」「AけどB」のように複数の観点を明示する時だけ、上位の
# episodic が属する会話糸を辿る。一般検索へ常時適用すると古い同糸を混ぜるため、
# 比較・変化・併記を求める問いに限定する。
FUKUSUU_ITO_RE = re.compile(
    r"両方|どっち|どう違|違った|比べ|比較|一方|それぞれ|前回.*(?:変わ|どう)|"
    r"あと(?:の|、)|たあと|した後|けど|けれど|だが|なのに|ままにする.*(?:新|別)",
    re.IGNORECASE,
)
FUKUSUU_THREAD_MAX = 3       # 起点にする上位episodicの会話糸
FUKUSUU_THREAD_KOUHO = 50   # リランカーへ渡す同糸候補の総上限
FUKUSUU_THREAD_SAIYO = 2    # 各糸から最終候補へ足す件数


def fukusuu_ito(query: str) -> bool:
    """複数記録を束ねる意図が、問いの表面に明示されているか。"""
    return bool(FUKUSUU_ITO_RE.search(str(query or "")))



def _kanten_bm25_ichii(kanten: str, items: list, hays: list, avgdl: float, nozoku=()):
    """1つの観点だけでBM25を回し、一位の記録を返す。決定論（LLMもリランカーも呼ばない）。

    nozoku に入っているキーは飛ばす＝A枠が取った記録をB枠が取らない（二重採用の防止）。
    """
    terms = query_terms(kanten)
    if not terms or not hays:
        return None
    total = max(1, len(items))
    idx_cache = _hay_index_for(hays, avgdl)
    kscores = _bm25_taat_scores(hays, avgdl, terms, total, idx_cache)
    n_items = len(items)
    ichii, saikou = None, 0.0
    for index in sorted(kscores):
        if index >= n_items:
            continue
        score = kscores[index]
        if score <= saikou:
            continue
        if nozoku and item_key(items[index]) in nozoku:
            continue
        ichii, saikou = items[index], score
    return ichii


def kanten_de_hirou(query: str, items: list, hays: list, avgdl: float,
                    results: list, diagnostics: dict | None = None,
                    _all_existing_keys: set[str] | None = None) -> list:
    """問いを観点A/Bに割り、各観点の一位を候補へ確保する。

    サカナAI(fugu)の設計「各観点を別々に検索し、最低1件ずつ確保してから統合する」の第2段。
    王が2つの事を一度に聞いた時（「AとB、両方どうなった？」）、今までは片方しか拾えなかった。

    ⚠️この段は**候補へ足すだけ**。リランカーの後で席を守る所までは作っていない
      （サカナの言う「観点ごとの枠を保持する」は次の段）。
    ⚠️割れない問いでは何もしない＝今までの検索を1ミリも変えない。
    失敗しても元の順位を返す（fail-open。検索を止めない）。
    """
    wareta = kanten_ni_wakeru(query)
    if not wareta:
        if diagnostics is not None:
            diagnostics["kanten"] = {"jotai": "warenai"}
        return results
    aru = set(_all_existing_keys) if _all_existing_keys is not None else {item_key(r) for r in results}
    waku, tsuika, totta = {}, [], set()
    try:
        # 「枠」= その観点の席。A枠が取った記録はB枠では取らない（二重採用の防止）。
        # ⚠️この診断の形(A_waku / B_waku)は、サカナAIの負試験が先に決めた契約。
        for namae, kanten in zip(("A_waku", "B_waku"), wareta):
            item = _kanten_bm25_ichii(kanten, items, hays, avgdl, nozoku=totta)
            if item is None:
                continue
            kagi = item_key(item)
            waku[namae] = kagi          # 既に候補に在っても「その観点の席」として記録する
            totta.add(kagi)
            if kagi in aru:
                continue                # 元の検索で拾えている＝足す必要は無い
            row = shiage(item, 0.0)
            row["kanten_hit"] = kanten
            tsuika.append(row)
            aru.add(kagi)
    except Exception as exc:        # fail-open。診断には型だけ残す
        if diagnostics is not None:
            diagnostics["kanten"] = {"jotai": "fail_open", "why": type(exc).__name__}
        return results
    if diagnostics is not None:
        diagnostics["kanten"] = {"jotai": "ugokashita" if tsuika else "tsuika_nashi",
                                 "kanten": list(wareta), "tsuika": len(tsuika), **waku}
    return results + tsuika


def fukusuu_thread_de_hirou(query: str, items: list[dict], results: list[dict],
                            diagnostics: dict | None = None,
                            _all_existing_keys: set[str] | None = None,
                            _lazy_tail_iter_fn=None) -> list[dict]:
    """上位episodicと同じ会話糸から、問いに合う別時点の記録を候補へ戻す。

    1件目を手掛かりに同じ thread_id の履歴を辿る one-hop retrieval。
    糸の全件を返さず、既存リランカーで問いと照合した上位だけを候補へ足す。
    失敗時は元の順位を返す（検索を止めない）。
    """
    if not results or not fukusuu_ito(query):
        if diagnostics is not None:
            diagnostics["fukusuu_thread"] = {"jotai": "hatsuka_sezu"}
        return results
    by_eid = {}
    for item in items:
        eid = str(item.get("event_id") or "") or str(item.get("id") or "").split(":")[0]
        if eid:
            by_eid[eid] = item
    threads = []
    for row in results:
        eid = str(row.get("event_id") or "") or str(row.get("id") or "").split(":")[0]
        item = by_eid.get(eid)
        thread = str((item or {}).get("thread_id") or row.get("thread_id") or "")
        if thread and thread not in threads:
            threads.append(thread)
        if len(threads) >= FUKUSUU_THREAD_MAX:
            break
    if len(threads) < FUKUSUU_THREAD_MAX and _lazy_tail_iter_fn is not None:
        for item in _lazy_tail_iter_fn():
            thread = str(item.get("thread_id") or "")
            if thread and thread not in threads:
                threads.append(thread)
                if len(threads) >= FUKUSUU_THREAD_MAX:
                    break
    if not threads:
        if diagnostics is not None:
            diagnostics["fukusuu_thread"] = {"jotai": "ito_nashi"}
        return results
    existing = set(_all_existing_keys) if _all_existing_keys is not None else {item_key(r) for r in results}
    siblings = [i for i in items
                if str(i.get("memory_type") or "") == "episodic"
                and str(i.get("thread_id") or "") in threads
                and item_key(i) not in existing]
    # 大きい全体糸でもGPUへの入力を無制限にしない。BM25+意味の本経路で上位に
    # 来た糸を優先し、各糸の新しい物から上限までを候補にする。
    siblings.sort(key=lambda i: (
        threads.index(str(i.get("thread_id") or "")),
        str(i.get("timestamp") or "")), reverse=False)
    if len(siblings) > FUKUSUU_THREAD_KOUHO:
        # 糸ごとの候補数を均す。一つの巨大糸に予算を独占させない。
        balanced = []
        per_thread = max(1, FUKUSUU_THREAD_KOUHO // len(threads))
        for thread in threads:
            same = [i for i in siblings if str(i.get("thread_id") or "") == thread]
            same.sort(key=lambda i: str(i.get("timestamp") or ""), reverse=True)
            balanced.extend(same[:per_thread])
        siblings = balanced[:FUKUSUU_THREAD_KOUHO]
    if not siblings:
        if diagnostics is not None:
            diagnostics["fukusuu_thread"] = {"jotai": "tsuika_nashi", "threads": len(threads)}
        return results
    if _is_cpu_agent_active() or (isinstance(diagnostics, dict) and diagnostics.get("brain_mode") == "cpu_agent"):
        if diagnostics is not None:
            diagnostics["fukusuu_thread"] = {"jotai": "skipped_cpu_agent", "threads": len(threads)}
        return results
    try:
        ranked = narabe_naosu(query, [shiage(i, 0.0) for i in siblings],
                              min(len(siblings), FUKUSUU_THREAD_KOUHO))
    except Exception as exc:  # fail-open。診断には型だけ残し、内部情報は公開しない。
        if diagnostics is not None:
            diagnostics["fukusuu_thread"] = {
                "jotai": "fail_open", "why": type(exc).__name__}
        return results
    chosen = []
    count = {thread: 0 for thread in threads}
    for row in ranked:
        eid = str(row.get("event_id") or "") or str(row.get("id") or "").split(":")[0]
        thread = str((by_eid.get(eid) or {}).get("thread_id") or "")
        if thread not in count or count[thread] >= FUKUSUU_THREAD_SAIYO:
            continue
        row = dict(row)
        row["fukusuu_thread_hit"] = True
        chosen.append(row)
        count[thread] += 1
    if diagnostics is not None:
        diagnostics["fukusuu_thread"] = {
            "jotai": "ugokashita" if chosen else "tsuika_nashi",
            "threads": len(threads), "kouho": len(siblings), "tsuika": len(chosen)}
    # 元の上位を壊さず、その直後へone-hop候補を置く。最終リランカーが全体を再照合する。
    return results[:1] + chosen + results[1:]


def imi_wo_yaku(root: Path, snapshot: dict) -> dict:
    """意味の索引を焼き直す。bge-m3が居ない・numpyが無い時は黙って諦める(脳は止めない)。

    2026-08-29 修理(バグ2): snapshot版を snapshot_ban としてimi側へ渡す(imi側がmetaへ保存する契約)。
    受け口がまだ無い旧imiでも壊れないよう、署名を見てから渡す。
    """
    try:
        import inspect
        import roundtable_imi
        items = searchable_items(snapshot)
        kwargs = {}
        try:
            if "snapshot_ban" in inspect.signature(roundtable_imi.tsukuru).parameters:
                kwargs["snapshot_ban"] = str(snapshot.get("version") or "")
        except (TypeError, ValueError):
            pass
        return roundtable_imi.tsukuru(root, items, sha_text, **kwargs)
    except Exception as error:
        return {"status": "skipped", "why": str(error)[:120]}


SHUDAI_HIROI_MAX = 5           # 1つの問いで引き戻す上限。多すぎると他の候補を押し出す


def shudai_de_hirou(root, query: str, items: list, results: list,
                    diagnostics: dict | None = None,
                    kagi_uke: list | None = None,
                    _lazy_promote_fn=None) -> list:
    """問いが名指しした主題の記録を、意味検索に押し下げられたまま終わらせない。

    2026-09-09 実測: 「自己点検.py は毎日何時に走るか」は BM25 だけなら1位なのに、
    意味を混ぜると上位8件が全部 chimu_selfcheck.py（別のファイル）になり、
    頭脳に7件在る当の記録が1件も出なかった。リランカーを落としても同じ。
    世界も同じ結論で、dense は「打った語を1つも含まない意味的に近い文書」を返す。
    定石どおり**完全一致の車線**を1本足す。主題キーが取れない問いでは何もしない。
    """
    if not query or not results:
        return results
    try:
        import sqlite3
        sys.path.insert(0, str(Path(__file__).resolve().parent))
        import fact_layer as _FL
        namae = _FL.jitsuzai_namae(root) if root else set()
        kagi = _FL.shudai_kagi(query, namae, _FL.jitsuzai_port(root) if root else None)
    except Exception as exc:                       # noqa: BLE001 拾えなくても検索は止めない
        if diagnostics is not None:
            diagnostics["shudai_hiroi"] = {"jotai": "skip", "why": str(exc)[:80]}
        return results
    if kagi_uke is not None:
        # 最終段の主題の席が「その名前が上位に出ているか」を見るために使う(2026-09-09)
        kagi_uke.extend(kagi)
    if not kagi:
        if diagnostics is not None:
            diagnostics["shudai_hiroi"] = {"jotai": "hatsuka_sezu", "kagi": []}
        return results

    if root is None:
        # 2026-09-15 ネオ ai-ab5f: root が無い時(hakaru.py --moji-only)に Path(None) が
        # TypeError で落ち、**文字検索だけで測るモードが丸ごと死んでいた**。
        # すぐ上の 2行(namae / kagi)は `if root else` で守っているのに、ここだけ守っていなかった。
        # root が無ければ索引DBは引けない＝docstring の「主題キーが取れない問いでは何もしない」に倣う。
        # ⚠️ root がある本番の経路は1バイトも変えていない。
        if diagnostics is not None:
            diagnostics["shudai_hiroi"] = {"jotai": "skip", "why": "root が無い(文字検索だけの測定)"}
        return results

    db = Path(root) / "roundtable" / "index" / "roundtable.db"
    hoshii = []
    try:
        con = sqlite3.connect("file:%s?mode=ro" % db, uri=True, timeout=5)
        try:
            q = ("SELECT event_id FROM seki_kagi WHERE kagi IN (%s) "
                 "ORDER BY ts DESC LIMIT ?" % ",".join("?" * len(kagi)))
            hoshii = [r[0] for r in con.execute(q, tuple(kagi) + (SHUDAI_HIROI_MAX * 3,))]
        finally:
            con.close()
    except Exception as exc:                       # noqa: BLE001 索引が無い旧DBでも動く
        if diagnostics is not None:
            diagnostics["shudai_hiroi"] = {"jotai": "sakuin_nashi", "why": str(exc)[:80],
                                           "kagi": kagi}
        return results
    if not hoshii:
        if diagnostics is not None:
            diagnostics["shudai_hiroi"] = {"jotai": "atarazu", "kagi": kagi}
        return results

    # ⚠️ items の id は "<event_id>:<sha>" の形で、索引の event_id と**完全一致しない**。
    #    2026-09-09: ここを == で比べていたため、索引にも items にも在るのに1件も拾えず、
    #    主題キーを29%増やしても数字が1つも動かなかった（自分の実装のバグ）。
    def _eid(x):
        return str(x.get("event_id") or "") or str(x.get("id") or "").split(":")[0]
    hoshii = hoshii[:SHUDAI_HIROI_MAX]
    motome = set(hoshii)

    # ① 既に候補に居る物は**先頭へ引き上げる**（元の相対順序は保つ）。
    #    「居ないなら足す」だけでは足りなかった ―― 実測で診断が sudeni_aru を返し、
    #    候補には居るのに意味検索に押し下げられて上位から落ちていた。
    ue, shita = [], []
    for r in results:
        if _eid(r) in motome:
            r["_shudai_hit"] = True   # 最終段の主題の席(shudai_no_seki)が見る印
            ue.append(r)
        else:
            shita.append(r)
    if _lazy_promote_fn is not None:
        for r in _lazy_promote_fn(motome):
            r["_shudai_hit"] = True
            ue.append(r)
    hikiageta = len(ue)
    aru = {_eid(r) for r in ue}

    # ② 候補にすら居ない物は items から拾って足す
    hiroeta, tarinai = [], {e for e in motome if e not in aru}
    if tarinai:
        for item in items:
            if _eid(item) in tarinai:
                hirotta = shiage(item, 0.0)
                hirotta["_shudai_hit"] = True
                hiroeta.append(hirotta)
                tarinai.discard(_eid(item))
                if not tarinai:
                    break

    if diagnostics is not None:
        diagnostics["shudai_hiroi"] = {
            "jotai": ("ugokashita" if (hikiageta or hiroeta) else "atarazu"),
            "kagi": kagi, "hikiageta": hikiageta, "hirotta": len(hiroeta)}
    if not (hikiageta or hiroeta):
        return results
    return ue + hiroeta + shita



IMI_KOUHO_KITEI = 30     # 意味検索から受け取る件数。環境 KINGDOM_IMI_KOUHO で変えられる。
#   ★なぜ栓にしたか(2026-09-14 ネオ ai-8742): 語が当たらない問い(王の話し言葉)は BM25 の順位が低く、
#     **意味の上位30件に入らなければ候補の上位に立てない**。RRFの加点は 1/(60+順位) なので、
#     31位以降は意味側の加点が丸ごと0になる。ここも「見せていない蓋」の一つ。
#   ⚠️広げる費用はほぼ0（索引は既にメモリ上・numpyの内積1回。並べ替え役のGPUは使わない）。
#     並べ替えの蓋(DAN1_KOUHO)を広げる費用とは桁が違う。


def imi_kouho() -> int:
    """意味検索から受け取る件数。呼ぶたび環境を読む(壊れた値は既定へ落ちる・脳は止めない)。"""
    try:
        n = int(os.environ.get("KINGDOM_IMI_KOUHO", str(IMI_KOUHO_KITEI)))
    except (TypeError, ValueError):
        return IMI_KOUHO_KITEI
    return max(1, n)


MOJI_DE_ATATTERU = 20.0
#   ★「意味では近い物が無い」が「王国にその話が無い」とは限らない(2026-09-14 ネオ ai-8742)。
#     固有名詞・ID・型番の問いは埋め込みが弱く、BM25だけが当てる。実測の出典は imi_wo_mazeru の注記。


def imi_wo_mazeru(root, query: str, items: list[dict], moji: list[dict],
                  diagnostics: dict | None = None,
                  chunk_pos_out: dict[str, int] | None = None) -> list[dict]:
    """文字で探した順位と、意味で探した順位を混ぜる(2026-08-20 ネオ)。

    なぜ要るか(実測): 文字の一致だけだと、王の「チムが嘘をついた話」が
    記録側の「捏造」「正直さ」に繋がらず、無関係な物が1位に出た。
    逆に意味だけだと固有名詞(型番・ポート番号)に弱い。**両方の順位を足す**のが素直で強い。
    重み調整の要らない RRF(順位の逆数の和)を使う。k=60は世で使われている既定値。

    bge-m3が落ちている・索引がまだ無い時は、文字の結果をそのまま返す(脳は止まらない)。
    """
    if root is None:
        if diagnostics is not None:
            diagnostics["semantic"] = {
                "state": "not_configured", "why": "root_is_none"}
        return moji
    try:
        import roundtable_imi
    except Exception as exc:
        if diagnostics is not None:
            diagnostics["semantic"] = {
                "state": "unavailable", "why": f"{type(exc).__name__}: {exc}"[:160]}
        return moji
    futa = imi_kouho()
    sagasu_limit = futa if "KINGDOM_IMI_KOUHO" in os.environ else max(futa, 150)
    try:
        sagasu_with_pos = getattr(roundtable_imi, "sagasu_with_chunk_pos", None)
        if callable(sagasu_with_pos):
            scores_map, best_cnum_map, info_map = sagasu_with_pos(root, query, sagasu_limit)
            jotai = str((info_map or {}).get("status") or "unknown")
            chikai = list((scores_map or {}).items())
            if chunk_pos_out is not None and isinstance(best_cnum_map, dict):
                chunk_pos_out.update(best_cnum_map)
        else:
            jotai, chikai = roundtable_imi.sagasu(root, query, limit=sagasu_limit)
    except Exception as exc:
        if diagnostics is not None:
            diagnostics["semantic"] = {
                "state": "unavailable", "why": f"{type(exc).__name__}: {exc}"[:160]}
        return moji
    if diagnostics is not None:
        diagnostics["semantic"] = {"state": str(jotai or "unknown"), "why": ""}
    if jotai == "ok" and not chikai:
        # 意味では1件も近くなかった。王国にその話が無い可能性が高い。
        # 文字だけがかすった物に「近い記録なし」の印を付ける(捨てはしない。判断は読む側がする)。
        # 2026-08-20 実測: 「北海道旅行の計画」に無関係な記録を1位で返していた。
        #
        # ★2026-09-14 ネオ ai-8742 が足した物 ―― **この印は当たる問いにも立つ。**
        #   実測(凍結土俵 base-20260828・golden_v2 240問): 意味検索が1件も返さない当たる問いが15本あり、
        #   **15本ぜんぶにこの印が立っていた。しかも10本は正解を1位で当てている。**
        #   (「dacf772はどこに刺したんだ？」「L1585って何の件だったか？」「1216²に上げた理由は何だ？」…
        #    全部**固有名詞・ID・型番**の問い。roundtable_imi の CHIKASA_MIN=0.50 に全部落ちる)
        #   ＝ **正解を手に持ったまま「王国に無い」と言いかけている。**
        #   印を消すと本当に無い時に言えなくなるので**消さない**。代わりに
        #   「**文字では強く当たっている**」という事実を添えて、読む側が見分けられるようにする。
        #   しきい値の出典(実測 2026-09-14・道具 /tmp/shirushi_wo_waru.py):
        #     意味0の当たる問い15本の文字の点 = 最低20.06 / 中央30.51 / 最高90.77
        #     印の立った無い問い33本        = 最低 6.48 / 中央14.75 / 最高36.96
        #     文字の点だけで見た分離 AUC = **0.9475**。しきい値20.0で**当たる問いの取りこぼし0/15**。
        #     (無い問いは5/33がこの印を持つ＝「文字では当たっている」と併記される。棄権はさせない)
        saikou = max((float(row.get("score") or 0.0) for row in moji), default=0.0)
        for row in moji:
            row["chikai_kiroku_nashi"] = True
            if saikou >= MOJI_DE_ATATTERU:
                row["moji_de_atatteru"] = round(saikou, 1)
        return moji
    if not chikai:
        return moji
    K = 60
    moji_keys = [item_key(row) for row in moji]
    have = set(moji_keys)
    tokuten: dict[str, float] = {}
    for rank, k in enumerate(moji_keys):
        tokuten[k] = tokuten.get(k, 0.0) + 1.0 / (K + rank)
    for rank, (key, _sim) in enumerate(chikai):
        if rank < futa or key in have:
            tokuten[key] = tokuten.get(key, 0.0) + 1.0 / (K + rank)
    # 融合加点(0.5*imi)と純意味候補の追加は従来通り上位futa件に絞り、
    # futa超の意味順位は文字一致候補のRRF救済(候補蓋50位内への押し上げ)にのみ使う。
    chikasa = dict(chikai[:futa])
    merged = list(moji)
    merged_keys = list(moji_keys)
    by_key = None
    _items_id = id(items)
    for _entry in _HAY_INDEX_OBOE.values():
        if _entry.get("items_id") == _items_id and _entry.get("by_key") is not None:
            by_key = _entry["by_key"]
            break
    if by_key is None:
        by_key = {item_key(item): item for item in items}
    for key, _sim in chikai[:futa]:
        if key in have:
            continue
        item = by_key.get(key)
        if item is not None:
            merged.append(shiage(item, 0))   # 文字では当たっていない。意味だけで拾った物
            merged_keys.append(key)
    for row, key in zip(merged, merged_keys):
        if key in chikasa:
            row["imi"] = round(chikasa[key], 3)
    order = sorted(range(len(merged)), key=lambda i: tokuten.get(merged_keys[i], 0.0), reverse=True)
    return [merged[i] for i in order]


_ORIG_IMI_WO_MAZERU = imi_wo_mazeru
_ORIG_SHIAGE = shiage
_ORIG_FUKUSUU_THREAD = fukusuu_thread_de_hirou
_ORIG_KANTEN_HIROU = kanten_de_hirou
_ORIG_SHUDAI_HIROU = shudai_de_hirou


PUBLIC_ABSTENTION_SCHEMA = "kingdom.roundtable.shared_brain.abstention.v1"
PUBLIC_ABSTENTION_POLICY = "fail_closed.no_answer.v1"
ROUND_TABLE_STATUS_QUERY = re.compile(
    r"今\s*(?:なに|何)\s*して|現在\s*(?:の)?\s*(?:状況|状態)|"
    r"(?:全体|共有脳|円卓|王国|作業)\s*(?:の)?\s*(?:状況|状態|ステータス)|"
    r"進行中\s*(?:の)?\s*(?:作業|もの|こと)",
    re.IGNORECASE,
)


def roundtable_digest_answers(query: str) -> bool:
    """円卓ダイジェスト自体が答えになる、明示的な全体状態の問いだけを認める。"""
    return bool(ROUND_TABLE_STATUS_QUERY.search(str(query or "")))


def public_retrieval_health(diagnostics: dict | None) -> dict:
    """内部例外文やURLを出さず、リクエスト固有の依存状態だけを公開する。"""
    diagnostics = diagnostics or {}
    return {
        "path": str(diagnostics.get("path") or "unknown"),
        "semantic": str((diagnostics.get("semantic") or {}).get("state") or "unknown"),
        "rerank": str((diagnostics.get("rerank") or {}).get("state") or "unknown"),
    }


def dependency_failure(diagnostics: dict | None) -> bool:
    """通常検索で、意味検索と再順位の両方が判断不能ならTrue。cpu_agent では False。"""
    if isinstance(diagnostics, dict) and diagnostics.get("brain_mode") == "cpu_agent":
        return False
    health = public_retrieval_health(diagnostics)
    if health["path"] != "regular":
        return False
    semantic_bad = health["semantic"] in {
        "skip", "fuicchi", "unavailable", "not_configured"}
    rerank_bad = health["rerank"] in {
        "fail_open", "tensuu_nashi", "zenmetsu", "unavailable"}
    return semantic_bad and rerank_bad


def dependency_independent_result(results: list[dict]) -> bool:
    """登録資産の文字直接一致は、意味検索・再順位なしでも同一性を検証できる。"""
    if not results:
        return False
    top = results[0]
    return bool(
        top.get("memory_type") == "media"
        and top.get("trust") == "registered_media"
        and top.get("attachments")
        and float(top.get("score") or 0) > 0
    )


MOJI_TSUYOSUGI_KITEI = 20.0
#   ★この点より強く文字で当たっていたら「王国に無い」とは言わない(2026-09-14 ネオ ai-8742)。
#
#   ★★2026-09-15 ネオ ai-abe8: **0.0 → 20.0 にした**(札 negai-20260914-125426-9181)。
#     出典 = reports/kiken_no_kyuusai_20260914/ (ネオ ai-9094 の実測・交差検証つき)
#       ・誤棄権(正解を候補に持つ222本のうち棄権した数): **16本(7.21%) → 1本(0.45%)**
#       ・**漏れた無い問い: 0本**(どのしきい値でも0)
#       ・式と実測が 276本×9点=2484通りで完全一致 ＝ 式を標本の外へ当ててよい
#       ・交差検証(split-half 4000通り): **0.0%(0/4000)** しか標本外の無い問いが漏れない
#     ⚠️ 37.0 ではなく 20.0 を選んだ理由: 37.0 は誤棄権 11本(4.95%)までしか下がらない。
#        20.0 の弱点(下記)は門番で塞げるので、誤棄権の少ない方を採った。
#
#     ⚠️**20.0 の弱点と、それを塞いだ門番**:
#       20.0 は「無い問い」33本のうち5本(37.0/32.0/24.9/22.9/20.3)を**文字の層で素通りさせ**、
#       漏れ0を **rerank の層だけ**が作っている。rerank は2026-09-14に294回OOMで落ちた実績がある。
#       → `rerank_taorete_iru()` を足し、**rerank が落ちている日は救済を無効へ倒す**。
#         試験 = /tmp/test_kiken_monban.py (14/14・「落ちた時に救済しない」と
#         「生きている時はちゃんと救済する」の**両側**を確認済み)
#
#     ⚠️ 切り替える時は必ず測り直す(33本は標本。無い問いが増えれば最高値は上がりうる)。
#        facts入り土俵での測り直しは**不要**と実証済み ――
#        base-20260828 と base-20260828-facts で per の zen_rank が276問とも一致
#        (reports/Code_facts_dohyou_kenshou_20260915.md)。1位が変わらない＝棄権の判定も変わらない。
#     37.0 の出典(実測 2026-09-14・道具 /tmp/shirushi_wo_waru.py):
#       印の立った「王国に無い問い」33本の文字の点の**最高が 36.96**。37.0 ならこの33本は1本も救わない
#       ＝ **未知の捕捉(33/36)を1本も落とさない**。一方で当たる問い15本のうち5本(37.18/38.35/45.78/47.94/90.77)を救う。
#     ⚠️33本は標本。無い問いが増えれば最高値は上がりうる。切り替える時は必ず測り直す。


def rerank_taorete_iru(diagnostics: dict | None) -> bool:
    """★再順位(rerank)の層だけが落ちているか（2026-09-15 ネオ ai-abe8）。

    なぜ要るか: `dependency_failure` は **semantic と rerank の両方**が悪い時しか真にならない。
    だが棄権の門番は二重で、**rerank の層だけ**が落ちても片側が死ぬ ――
    `public_abstention` の最後は `if "rerank" not in top: return None` ＝ **棄権しない**。
    そこへ文字の救済(`moji_de_tsuyosugiru`)が効いていると、
    **3番目の門も4番目の門も両方抜けて、王国に無い問いに答えてしまう。**

    実測の裏づけ(reports/kiken_no_kyuusai_20260914/ ・ネオ ai-9094):
      しきい値 20.0 は「無い問い」33本のうち5本(37.0/32.0/24.9/22.9/20.3)を
      **文字の層で素通りさせ、漏れ0を rerank の層だけが作っている**。
      rerank は2026-09-14に実際に294回OOMで落ちた実績がある（その日この守りが消える）。
    """
    health = public_retrieval_health(diagnostics)
    return health["rerank"] in {"fail_open", "tensuu_nashi", "zenmetsu", "unavailable"}


def moji_tsuyosugi() -> float:
    """棄権を止める文字の点。呼ぶたび環境を読む(壊れた値は既定へ落ちる・脳は止めない)。"""
    try:
        return float(os.environ.get("KINGDOM_MOJI_TSUYOSUGI", str(MOJI_TSUYOSUGI_KITEI)))
    except (TypeError, ValueError):
        return MOJI_TSUYOSUGI_KITEI


def moji_de_tsuyosugiru(results: list[dict], diagnostics: dict | None = None) -> bool:
    """1位が「文字で強く当たりすぎている」か。0.0 なら常に False ＝ 今までどおり。

    ★2026-09-15 ネオ ai-abe8: **rerank の層が落ちている時は救済しない**（安全側へ倒す）。
      この救済は「漏れ0」を rerank の層に頼っている。片側が死んだまま救済を効かせると
      **二重の門番が両方とも抜ける**。落ちた日は今までどおり棄権する方が正しい。
    """
    if rerank_taorete_iru(diagnostics):
        return False
    shikii = moji_tsuyosugi()
    if shikii <= 0 or not results:
        return False
    try:
        if not (float(results[0].get("moji_de_atatteru") or 0.0) >= shikii):
            return False
    except (TypeError, ValueError):
        return False
    # ★2026-11-24 第11波トラック1 続報（OPT2F-Guard-v2: 案①と対になる抜け穴封鎖）:
    #   出典 = work/kukuru_heya/wave11_track1_opt2_design_eval_20261123.md
    #   案①により Stage-2 top1 rerank >= 0.010 の A/D 誤フラグは既に chikai_kiroku_nashi 自体が解除済み。
    #   一方、規模土俵(9.39倍)では BM25 上振れにより B canary 7/8問がここを素通りし、
    #   特に 0.005 <= rerank < 0.010 の帯域では no_semantic_match と imi_nashi_low_rerank の両門を抜ける
    #   潜在的抜け穴が残存していた。0 < rr < kaijo_thr の低関連度候補は文字スコアだけで素通りさせず
    #   1段目 no_semantic_match で確実に捕捉する（rr <= 0.0 は既存 zero_rerank_relevance 契約維持）。
    kaijo_thr = nashi_rerank_kaijo_shikii()
    if kaijo_thr > 0.0 and isinstance(results[0], dict) and "rerank" in results[0]:
        try:
            rr = float(results[0].get("rerank"))
        except (TypeError, ValueError):
            return False
        if not (rr >= kaijo_thr or rr <= 0.0):
            return False
    return True


# ★2026-11-23 第11波トラック1（案①: Stage-2 Top-1 rerank による chikai_kiroku_nashi 誤フラグ解除）:
#   出典 = work/kukuru_heya/wave11_track1_ruri_abstention_gate_20261123.md
#   凍結276問の chikai_kiroku_nashi=True 48問（B種33問 vs A/D誤フラグ15問）上で、
#   既常駐 Stage-2 ruri-v3-reranker-310m の Top-1 スコアは B種最大 0.0040 vs A/D誤フラグ最小 0.0160
#   （4.0倍ギャップ・AUC=1.00000）。規模門40問（9.39倍コーパス）でも B種最大 0.0020 vs A/D最小 0.2120 と
#   完全スケール不変。追加 VRAM +0 MiB・追加推論 +0 ms。
#   ⚠️ fail-safe: rerank 障害時や top1 に rerank 欄が無い時は一切解除せず chikai_kiroku_nashi を維持する。
NASHI_RERANK_KAIJO_KITEI = 0.010


def nashi_rerank_kaijo_shikii() -> float:
    """Stage-2 Top-1 rerank による chikai_kiroku_nashi 解除しきい値。0.0 以下で無効。"""
    try:
        return float(os.environ.get("KINGDOM_NASHI_RERANK_KAIJO", str(NASHI_RERANK_KAIJO_KITEI)))
    except (TypeError, ValueError):
        return NASHI_RERANK_KAIJO_KITEI


def _rerank_de_nashi_wo_kaijo(rows: list[dict], diagnostics: dict | None = None) -> list[dict]:
    """Stage-2 Top-1 rerank が十分高い時のみ chikai_kiroku_nashi 誤フラグを解除する（fail-safe付き）。"""
    shikii = nashi_rerank_kaijo_shikii()
    if shikii <= 0.0 or not rows:
        return rows
    if rerank_taorete_iru(diagnostics):
        return rows
    if not any(isinstance(r, dict) and r.get("chikai_kiroku_nashi") for r in rows):
        return rows
    top = rows[0] if isinstance(rows[0], dict) else None
    if top is None or "rerank" not in top:
        return rows
    try:
        rr = float(top.get("rerank"))
    except (TypeError, ValueError):
        return rows
    if not (rr >= shikii):
        return rows
    for row in rows:
        if isinstance(row, dict):
            row.pop("chikai_kiroku_nashi", None)
    if diagnostics is not None:
        diagnostics["nashi_rerank_kaijo"] = {
            "cleared": True,
            "top1_rerank": round(rr, 4),
            "threshold": shikii,
        }
    return rows


# 第18波/第19波ヘルパー本体は scripts/brain_cpu_agent.py へ分離し上部で再エクスポート。

def public_abstention(results: list[dict], diagnostics: dict | None = None) -> dict | None:
    """公開応答で、無関係な候補を「答え」として流さないための門番。

    ``search_snapshot`` は評価・原因調査に候補そのものが必要なので生のまま残す。
    公開境界(search/context/MCP)だけがこの判定を使い、棄権時は候補本文を返さない。

    閾値の根拠(凍結土俵 base-20260828、2026-09-02再計算):
      - chikai_kiroku_nashi のみ: 未知33/36、既知の誤棄権15/240 (6.25%)
      - 上記 + top1 rerank == 0: 未知35/36、既知の誤棄権22/240 (9.17%)
      - rerank <= 0.001: 誤棄権31/240 (12.92%)となり上限10%を破るため不採用

    rerank 欄が無い時はリランカー障害/未使用の可能性があるため、0点と同一視しない。
    """
    dependencies = public_retrieval_health(diagnostics)
    dependency_unavailable = (
        dependency_failure(diagnostics) and not dependency_independent_result(results))
    if dependency_unavailable:
        reason = "dependency_unavailable"
        signal = "semantic_and_rerank_unhealthy"
        message = "共有脳の検索依存機能を確認できないため、安全のため回答を棄権します。"
    elif not results:
        reason = "no_candidates"
        signal = "empty_search"
        message = "共有脳に、この問いへ返せる記録がありません。"
    elif isinstance(diagnostics, dict) and diagnostics.get("brain_mode") == "cpu_agent":
        return None
    elif (any(bool(row.get("chikai_kiroku_nashi")) for row in results)
          and not moji_de_tsuyosugiru(results, diagnostics)):
        # ★2026-09-14(ネオ ai-8742): 「意味で近い物が無い」＝「王国にその話が無い」ではない。
        #   実測(凍結土俵240問): 意味検索が1件も返さない**当たる問い15本ぜんぶ**にこの印が立ち、
        #   **うち10本は正解を1位で当てていた**(固有名詞・ID・型番の問い)。
        #   注記にある取引「未知33/36 と引き換えに誤棄権15/240(6.25%)」を、
        #   **未知を1本も落とさずに** 誤棄権 15→10(4.17%) へ改善できる見込み。
        #   ★moji_de_tsuyosugiru() が真でもここを抜けるだけで、**下の rerank の判定は受ける**
        #     (関連度0なら結局棄権する)。守りは二重のまま。
        reason = "no_semantic_match"
        signal = "chikai_kiroku_nashi"
        message = "共有脳に、意味の近い記録がありません。"
    else:
        top = results[0]
        if "rerank" not in top:
            return None
        try:
            rr = float(top.get("rerank"))
        except (TypeError, ValueError):
            rr = None
        zero_relevance = (rr is not None and rr <= 0.0)
        # 2026-09-04 追加(ネオ): 関連度がほぼ無く、意味でも近い物がゼロの時は棄権する。
        #
        # なぜこの二つか(凍結土俵 base-20260828 / golden_v2 276問で実測):
        #   top1の「意味で近い物がゼロ」割合 … 棄権すべきB群 97.2% / 答えるべきA群 12.1% D群 19.5%
        #   条件「rerank<0.005 かつ imi無し」に入る割合 … B 94.4% / A 1.5% / D 1.1%
        #   つまりこの二つはB群をよく分ける。
        # Claude案の rr_yugo は**分離に効かなかった**ので採らない(同日実測):
        #   rerank<0.010 で rr_yugo を 0.0〜-3.9 まで動かしても誤棄権は 37/240 のまま1件も変わらず、
        #   rerank<0.005 でも 25/240 のまま動かなかった。18通り試して受入を満たす組は無かった。
        # rerank単独で 0.005 まで緩めると誤棄権10.4%で上限を破るため、imi無しとの二条件にしている。
        # ★2026-09-14(ネオ ai-16ab): 二観点の問いは**問い全体で引くと**意味に当たらないが、
        #   **観点ごとに引けば当たる**(サカナAIの実測)。観点の席が意味の車線で当てた証拠
        #   (kanten_imi)が結果に在るなら「意味で近い物がゼロ」とは言えないので棄権しない。
        #   ⚠️関連度0(zero_relevance)の棄権は触らない。割れない問いでは印が付かない＝素通り。
        kanten_de_atatta = any(row.get("kanten_imi") for row in results)
        imi_nashi_yowai = (rr is not None and rr < 0.005 and top.get("imi") is None
                           and not kanten_de_atatta)
        spec_thr = low_rr_spec_abstain_shikii()
        spec_cov = diagnostics.get("spec_coverage") if isinstance(diagnostics, dict) else None
        spec_missing_yowai = False
        if (
            spec_thr > 0.0
            and rr is not None
            and math.isfinite(rr)
            and rr < spec_thr
            and not rerank_taorete_iru(diagnostics)
            and not kanten_de_atatta
            and isinstance(spec_cov, dict)
        ):
            try:
                sc_cnt = int(spec_cov.get("spec_count") or 0)
                sc_cov = float(spec_cov.get("top3_coverage"))
                if sc_cnt >= 1 and math.isfinite(sc_cov) and sc_cov < LOW_RR_SPEC_COV_KITEI:
                    spec_missing_yowai = True
            except (TypeError, ValueError):
                spec_missing_yowai = False
        if not (zero_relevance or imi_nashi_yowai or spec_missing_yowai):
            return None
        if zero_relevance:
            reason = "zero_rerank_relevance"
            signal = "top1_rerank_eq_0"
            message = "共有脳の候補は、この問いへの関連性を確認できませんでした。"
        elif imi_nashi_yowai:
            reason = "imi_nashi_low_rerank"
            signal = "top1_low_rerank_and_no_imi"
            message = "共有脳の候補は、この問いへの関連性を確認できませんでした。"
        else:
            reason = "low_rerank_spec_missing"
            signal = "top1_low_rerank_and_spec_missing"
            message = "共有脳の候補は、この問いの核心語と関連性を確認できませんでした。"
    out = {
        "schema": PUBLIC_ABSTENTION_SCHEMA,
        "policy": PUBLIC_ABSTENTION_POLICY,
        "reason": reason,
        "signal": signal,
        "message": message,
        "candidate_count": len(results),
    }
    if dependency_unavailable:
        out["dependencies"] = dependencies
    return out


def public_results(results: list[dict], diagnostics: dict | None = None) -> tuple[list[dict], dict | None]:
    """公開可能な結果と棄権情報を返す。入力候補は変更しない。"""
    abstention = public_abstention(results, diagnostics)
    return ([] if abstention else results), abstention


# ===== 段D: 呼び出し回数の記録 2026-09-04 ネオ =====
# 王命2026-09-04(work_orders/王命_20260904-0421_...md)の段D。
#
# 何のための物か: 「使わない物は奥へ、よく使う物は手前へ」を将来やるための**材料集め**。
#   今の並べ替えは日数(鮮度)しか見ないので、古くてもよく使う記録が沈む。
#   人の記憶に近づけるなら使用頻度が要る。ただし**今は数えるだけで、順位には一切効かせない**
#   (王命「加点は入れない。効果を凍結土俵で測ってから別途裁可」)。
#
# 壊れない作り:
#   - 追記のみのjsonl。Linuxの O_APPEND は1行が PIPE_BUF(4096B)以下なら分断されないので、
#     5分毎のrefreshとMCPの検索が同時に走っても行が混ざらない。1行は200字程度に収める。
#   - 書けなくても検索は止めない(fail-open)。頭脳より台帳が大事になる事は無い。
#   - 集計は後から yobareta_shukei() で読む。書く側は数えない(書き込みを軽く保つ)。

# ===== 足あとの遅れ書き（2026-09-27 ネオ ai-ff7a・王命_20260927-1829）=====
# なぜ要るか:
#   「読むだけ(readOnlyHint=True)」の印が付いた頭脳の口2つ
#   （roundtable_brain_search / roundtable_brain_context）が、**呼ぶたび必ず**
#   yobareta.jsonl と kiken.jsonl へ追記していた（2026-09-27 に実呼出しで4/4回 実測）。
#   MCP の readOnlyHint は「the tool does not modify its environment」であり、
#   世界は「**問い合わせを記録する検索ツールは read-only ではない**」と明言している。
#
# どう直したか（王命の「記録経路分離を優先」）:
#   記録をやめない。**呼び出しの経路から外す。**その場では積むだけにし、
#   背後の手が少し後でまとめて書く。世界の定石そのまま ――
#   「テレメトリを要求の経路で同期に処理するのは論外。集めるのと書き出すのを切り離し、
#     手元に溜めてまとめて出す」（OneUptime / AWS Lambda の post-response flush ほか）。
#
# ★世界が同時に釘を刺している事:
#   「遅らせた仕事は要求の文脈の外で走るので、**失敗が呼び手へ返らない**。
#     書き出しの失敗は必ず記録して知らせろ。でないと一番まずい時に観測が穴になる」
#   → だから _ASHIATO_JOUTAI に積んだ数・書けた数・落ちた数・最後の理由を残し、
#     既存の YOBARETA_JOUTAI / KIKEN_JOUTAI と同じく見張りが読めるようにする。
#
# 正直に言う限界:
#   遅れて**書かれはする**ので、厳密な意味の read-only ではない。
#   満たしているのは「**口の呼び出しの前後で台帳が変わらない**」ところまで。
#   プロセスが荒く落ちれば、積んだまま書けていない分は失われる（atexit と 1秒ごとの
#   書き出しで幅を小さくしている）。失った事は _ASHIATO_JOUTAI に残る。
# ④ 囲い（2026-10-18 ネオ ai-d0cf）: 見せる相手を限った棚（scripts/nirai_kakoi.py）を引く時は、
#   足あと・壊れ行の警告を標準エラーへ出さない。標準エラーは systemd が mcp/mcp_server.log へ流し、
#   その記録はどの名義でも読める（観測の関の logs）＝囲いの問い・ID・道が全員に見えてしまう（反証2回目 H3）。
#   印は contextvars なので、同じプロセスの別の呼び出し（別スレッド・別の非同期の流れ）には効かない。
#   印を立てない呼び出し（今の全部の呼び出し）は今と1字も変わらない。
_SOTO_NI_DASANAI = contextvars.ContextVar("roundtable_brain_soto_ni_dasanai", default=False)


@contextlib.contextmanager
def soto_ni_dasanai():
    """この中の呼び出しは、王国の記録（標準エラー）へ何も出さない。"""
    tok = _SOTO_NI_DASANAI.set(True)
    try:
        yield
    finally:
        _SOTO_NI_DASANAI.reset(tok)


_ASHIATO_LOCK = threading.Lock()
_ASHIATO_JOUTAI = {"state": "-", "at": "", "why": "", "tsunda": 0, "ochita": 0}
ASHIATO_SHIRUSHI = "【足あと】"


def _ashiato_tsumu(michi: str, gyou: str) -> None:
    """★台帳へ書かない。**プロセスの観測として標準エラーへ1行出すだけ。**

    ここが王命_20260927-1829 の芯。前はここで台帳へ直に追記していたので、
    「読むだけ」の印が付いた口が**呼ぶたび環境を変えて**いた。

    ★同じプロセスの中で遅らせるだけでは足りない事を実測で確かめた（2026-09-27）――
      1秒ごとの背後の手にすると、**次の呼び出しの最中に走って**契約試験に捕まる。
      口の呼び出しと同じプロセスで台帳へ触る限り、read-only にはならない。

    だから**出口を変えた**。標準エラーは systemd が mcp/mcp_server.log へ流す＝
    王国のコードの `open()` を通らない。ログは**プロセスの観測**であって王国のデータではない。
    台帳（yobareta.jsonl / kiken.jsonl）へ書くのは**別プロセス**
    `scripts/ashiato_wo_hirou.py`（1分ごと）の仕事にした。

    世界の定石そのまま ―― 「テレメトリを要求の経路で同期に処理するのは論外。
    集めるのと書き出すのを切り離せ」。そして世界の釘「遅らせた仕事の失敗は呼び手へ返らない。
    必ず記録して知らせろ」に従い、出せなかった事は _ASHIATO_JOUTAI に残す。
    """
    if _SOTO_NI_DASANAI.get():
        return                       # ④ 囲いの中の検索: 足あとを出さない（囲いの台帳は拾い手の外＝元から書かれない）
    try:
        root = _ashiato_no_ne(michi)
        sys.stderr.write(ASHIATO_SHIRUSHI + json.dumps(
            {"michi": root, "gyou": gyou}, ensure_ascii=False) + "\n")
        sys.stderr.flush()
        with _ASHIATO_LOCK:
            _ASHIATO_JOUTAI["tsunda"] += 1
            _ASHIATO_JOUTAI.update({"state": "dashita", "at": _ima_d(), "why": ""})
    except Exception as exc:  # noqa: BLE001 ― 足あとで脳を止めない
        with _ASHIATO_LOCK:
            _ASHIATO_JOUTAI["ochita"] += 1
            _ASHIATO_JOUTAI.update({"state": "dasenakatta", "at": _ima_d(),
                                    "why": ("%s: %s" % (type(exc).__name__, exc))[:120]})


def _ashiato_no_ne(michi: str) -> str:
    """台帳の道を王国の家からの相対にする（拾う側が家を決める＝道を跨がせない）。"""
    m = os.path.abspath(michi)
    ie = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
    return os.path.relpath(m, ie) if m.startswith(ie + os.sep) else m


def ashiato_joutai() -> dict:
    """見張り用。出せた数・出せなかった数・最後の理由。

    ★世界の釘: 遅らせた仕事の失敗は呼び手へ返らない。出せなかった事は必ず残して知らせろ。
      でないと一番まずい時に観測が穴になる。
    """
    with _ASHIATO_LOCK:
        return dict(_ASHIATO_JOUTAI)


YOBARETA_LOG = "roundtable/brain/yobareta.jsonl"
YOBARETA_JOUTAI: dict = {"state": "未実行", "at": "", "why": ""}


def _yobidashimoto() -> str:
    """名乗らずに引かれた時、**どこから呼ばれたか**を自動で残す（2026-09-14 ネオ ai-6821）。

    実測: yobareta.jsonl 16,630行のうち **10,450行(62.8%)が who 空**だった。
    空のままでは「頭脳を使った」を証明できない（くくるP5 Receiptの指摘）。
    呼び出し側を直すのが本筋だが、**直し忘れても自動で残る**形にして根を塞ぐ。
    この関数自身と roundtable_brain.py の内側は飛ばして、外の最初の呼び手を採る。
    """
    try:
        import traceback
        jibun = os.path.basename(__file__)
        for fr in reversed(traceback.extract_stack()[:-1]):
            na = os.path.basename(fr.filename)
            if na and na != jibun:
                return "?" + na[:18]
    except Exception:                      # noqa: BLE001 ― 名乗り探しで検索を止めない
        pass
    return "?不明"


CHAT_ID_HASH_KETA = 16      # 桁数はここだけ。増やしたくなったらこの数だけ動かす
HELP_CHAT_ID = "会話の相関ID（生のまま保存せず sha256 の先頭16桁で残す。既定は空＝今までと同じ）"


def chat_id_hash(nama: str) -> str:
    """会話の相関ID。生の chat_id は**保存しない**（くくるの制約 2026-09-24）。

    出力は必ず [0-9a-f]{CHAT_ID_HASH_KETA}、**ただし中身が無い物(空・空白だけ・None)は空**。
    空を返すのは「この問い合わせに会話IDが無い」という意味で、受領書には欄ごと出さない。
    長さも文字種も構造で決まるので、
    検証も切り詰めも要らず、変な字が台帳へ入る道が無い。
    ⚠️ trace_id とは別物。trace_id は「1回の問い合わせ」、これは「同じ会話」。
       混ぜると OpenTelemetry の trace_id/session.id を混同したのと同じ誤りになる。
    ★算出はこの関数の1本だけ。読む側(KingRealQuery)も必ずこれを呼ぶ。
    """
    s = str(nama or "").strip()
    if not s:
        return ""
    import hashlib
    return hashlib.sha256(s.encode("utf-8")).hexdigest()[:CHAT_ID_HASH_KETA]


def yobareta_kiroku(root, query: str, results: list[dict], participant: str = "", trace_id: str = "", chat_id_hash_: str = "") -> None:
    """検索が返した記録の身元を1行だけ書き残す。順位には一切効かせない。

    「返された回数」だけを数える。**「返答に実際に使われた回数」は数えていない**
    (検索側からは、読み手のAIがその記録を使ったかどうか分からない。
     数えられない物を数えたふりをしない。使用の記録は別の口が要る)。
    """
    if root is None or not results:
        return
    try:
        import io
        michi = os.path.join(str(root), YOBARETA_LOG)
        mi = []
        for i, r in enumerate(results[:5], 1):
            key = str(r.get("record_id") or r.get("event_id") or r.get("id")
                      or r.get("path") or "")
            if key:
                mi.append({"i": i, "id": key[:80]})
        if not mi:
            return
        # 名乗りが空なら、呼び出し元のファイル名を入れる（"空" を作らない）
        dare = (participant or "").strip() or _yobidashimoto()
        _hon = {
            "at": datetime.now().astimezone().isoformat(timespec="seconds"),
            "trace_id": (trace_id or "")[:16],
            "q": (query or "")[:60],
            "who": dare[:20],
            "kaeshita": mi,
        }
        if chat_id_hash_:
            _hon["chat_id_hash"] = chat_id_hash_
        gyou = json.dumps(_hon, ensure_ascii=False)
        if len(gyou.encode("utf-8")) > 3900:      # PIPE_BUF(4096)の内側に収める
            _chi = {"at": datetime.now().astimezone().isoformat(timespec="seconds"),
                    "trace_id": (trace_id or "")[:16],
                    "q": (query or "")[:20], "who": dare[:20],
                    "kaeshita": mi[:2]}
            if chat_id_hash_:
                _chi["chat_id_hash"] = chat_id_hash_
            gyou = json.dumps(_chi, ensure_ascii=False)
        _ashiato_tsumu(michi, gyou)      # ★その場では書かない（王命_20260927-1829）
        YOBARETA_JOUTAI.update({"state": "ok", "at": _ima_d(), "why": ""})
    except Exception as exc:
        # 台帳が書けなくても脳は止めない。ただし落ちた事は残す(無言のfail-openにしない)
        YOBARETA_JOUTAI.update({"state": "fail_open", "at": _ima_d(),
                                "why": f"{type(exc).__name__}: {exc}"[:120]})


def _ima_d() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


def yobareta_shukei(root, hi_kara: str = "", hi_made: str = "") -> dict:
    """台帳を読んで「どの記録が何回返されたか」を数える。期間で切れる。

    期間で切れるようにした理由(2026-08-26の教訓): 通算しか数えない台は、
    直しても永久に良くなったと言えない。
    """
    michi = os.path.join(str(root), YOBARETA_LOG)
    kazu: dict = {}
    ichii: dict = {}
    gyousu = 0
    try:
        with open(michi, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    row = json.loads(line)
                except Exception:
                    continue          # 壊れた行は飛ばす(1行の破損で全体を失わない)
                at = str(row.get("at") or "")
                if hi_kara and at < hi_kara:
                    continue
                if hi_made and at > hi_made:
                    continue
                gyousu += 1
                for m in row.get("kaeshita") or []:
                    key = str(m.get("id") or "")
                    if not key:
                        continue
                    kazu[key] = kazu.get(key, 0) + 1
                    if m.get("i") == 1:
                        ichii[key] = ichii.get(key, 0) + 1
    except FileNotFoundError:
        return {"schema": "kingdom.brain.yobareta.v1", "gyousu": 0,
                "kensuu": 0, "why": "台帳がまだ無い", "joui": []}
    joui = sorted(kazu.items(), key=lambda kv: -kv[1])[:20]
    return {
        "schema": "kingdom.brain.yobareta.v1",
        "kikan": {"kara": hi_kara or "(最初から)", "made": hi_made or "(今まで)"},
        "gyousu": gyousu,                  # 検索が何回走ったか
        "kensuu": len(kazu),               # 何種類の記録が返されたか
        "joui": [{"id": k, "kaesareta": v, "ichii": ichii.get(k, 0)} for k, v in joui],
    }



# ===== 棄権の口（王命_20260921-0059 P2・2026-09-21 ネオ ai-91bb）=====
# 直した穴: **本番で棄権しても台帳に1行も残らなかった。**
#   実測(2026-09-21 11:1x・newpc):
#     ・search  車線 … abstained=True / reason=zero_rerank_relevance → yobareta.jsonl に 0 行
#     ・context 車線 … abstained=True / reason=imi_nashi_low_rerank  → yobareta.jsonl に 0 行
#   根は2つ。①yobareta_kiroku は `not results` で即 return するが、棄権は results を空にする所
#   ＝**棄権だけが必ず記録から落ちる**。②context 車線はそもそも1行も書いていなかった。
#   結果、物差し zunou_kenkou_12.py の「棄権」は ✗取れない のままだった。
#
# 世界の作法（2026-09-21 に調べた。ディープサーチの結果を要約して置く）:
#   ①棄権は「refusal log」として**1件ずつ・安定した理由タグ付きで**残す
#     (Abstention Recipe / RAGPinCheng PR#825「abstention observability loop = refusal log + aggregate API」。
#      理由タグは no_sources / low_confidence / not_found / guard のような固定語にする)
#   ②率の分母は**処理した全問い合わせ**。Reject Rate RR = N_R / N (OECD.AI メトリクス目録)
#     → だから答えた時も1行書く。答えた分を書かないと分母が作れない。
#   ③selective prediction の作法では coverage(答えた割合) と risk(答えた中の誤り) は**別の数**。
#     1つに畳まない。
#   ★④**棄権率だけでは「正しい棄権か誤棄権か」は判定できない。**
#     (arXiv:2510.01782「refusal 決定はモデルの自己認識だけで決まらない。単に refusal rate を
#      数えるだけでは knowledge-aware refusal を評価できない」)
#     ＝**この口から誤棄権率を名乗ってはいけない。**誤棄権はラベル付きの土俵でしか測れない
#     (王国では reports/kiken_no_kyuusai_20260914/ の 276問。GPUが要る)。
#
# 壊れない作り（yobareta と同じ掟）:
#   - 追記のみ・1行は短く保つ(PIPE_BUF 4096B の内側)。同時に走っても行が混ざらない
#   - 書けなくても検索は止めない(fail-open)。ただし落ちた事は残す(無言で落とさない)
#   - 数えるのは後から kiken_shukei() で。書く側は数えない
#   - **yobareta.jsonl とは別の台帳にする。**yobareta の 32,000 行は棄権を1行も持っていないので、
#     混ぜると「棄権0件の古い行」が分母に入り、棄権率が**実際より低く**出る
#     (＝甘い方へ倒れる数え方。一番危ない。2026-09-21 ai-6162 の罠1)
KIKEN_LOG = "roundtable/brain/kiken.jsonl"
KIKEN_JOUTAI: dict = {"state": "未実行", "at": "", "why": ""}


def kiken_kiroku(root, query: str, kuchi: str, abstention: dict | None,
                 participant: str = "", trace_id: str = "",
                 kensaku_kiken: dict | None = None, chat_id_hash_: str = "") -> None:
    """公開境界を1回通るたびに「答えたか・棄権したか」を1行残す。

    棄権した時だけでなく**答えた時も書く**。分母(処理した全問い合わせ)を
    同じ台帳・同じ開始時刻で作るため（世界の作法②）。順位・点数には一切効かせない。

    kuchi           … "search" か "context"（どの公開口か）
    abstention      … 応答そのものの棄権(None なら答えた)
    kensaku_kiken   … context 車線で「検索は棄権したがダイジェストが答えた」時の検索側の棄権。
                      これは**応答の棄権ではない**ので kekka は kotaeta のまま、別欄に残す。
    """
    if root is None:
        return
    try:
        import io
        michi = os.path.join(str(root), KIKEN_LOG)
        dare = (participant or "").strip() or _yobidashimoto()
        ab = abstention if isinstance(abstention, dict) else None
        row = {
            "at": datetime.now().astimezone().isoformat(timespec="seconds"),
            "trace_id": (trace_id or "")[:16],
            "q": (query or "")[:60],
            "who": dare[:20],
            "kuchi": (kuchi or "?")[:12],
            "kekka": "kiken" if ab else "kotaeta",
        }
        if chat_id_hash_:
            row["chat_id_hash"] = chat_id_hash_
        if ab:
            row["riyuu"] = str(ab.get("reason") or "unknown")[:40]
            row["signal"] = str(ab.get("signal") or "")[:40]
            row["kouho"] = ab.get("candidate_count")
        if isinstance(kensaku_kiken, dict) and not ab:
            # 検索レーンだけが棄権し、正本ダイジェストが答えた場合。
            # ★ここを kekka=kiken にすると「答えたのに棄権と数える」＝甘い方ではなく
            #   辛い方へ外れるが、いずれにせよ嘘なので分けて持つ。
            row["kensaku_kiken"] = str(kensaku_kiken.get("reason") or "unknown")[:40]
        gyou = json.dumps(row, ensure_ascii=False)
        if len(gyou.encode("utf-8")) > 3900:          # PIPE_BUF(4096)の内側に収める
            row["q"] = (query or "")[:20]
            gyou = json.dumps(row, ensure_ascii=False)
        _ashiato_tsumu(michi, gyou)      # ★その場では書かない（王命_20260927-1829）
        KIKEN_JOUTAI.update({"state": "ok", "at": _ima_d(), "why": ""})
    except Exception as exc:                           # noqa: BLE001 ― 台帳で脳を止めない
        KIKEN_JOUTAI.update({"state": "fail_open", "at": _ima_d(),
                             "why": f"{type(exc).__name__}: {exc}"[:120]})


def kiken_shukei(root, hi_kara: str = "", hi_made: str = "") -> dict:
    """棄権の台帳を読んで Reject Rate を出す。**数え方を必ず結果に書いて返す。**

    返す物:
      bunbo      … 処理した問い合わせ数(答えた+棄権した)。世界の作法②の N
      bunshi     … 棄権した数。N_R
      ritsu      … bunshi/bunbo。**分母0なら 0.0 ではなく None**(測れていない 0 を名乗らない)
      kaishi     … この台帳の最初の行の時刻。**これより前は数えていない**
      hamidashi  … 頼まれた期間が kaishi より前から始まっている(率はその分を含まない)
      riyuu_betsu/kuchi_betsu … 内訳
    ★誤棄権率ではない。正しい棄権かどうかはこの台帳では分からない(世界の作法④)。
    """
    michi = os.path.join(str(root), KIKEN_LOG)
    bunbo = bunshi = kowareta = 0
    riyuu: dict = {}
    kuchi: dict = {}
    kensaku_dake = 0
    kaishi = ""
    saigo = ""
    try:
        with open(michi, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    row = json.loads(line)
                except Exception:
                    kowareta += 1
                    continue                 # 1行の破損で全体を失わない
                at = str(row.get("at") or "")
                if at and (not kaishi or at < kaishi):
                    kaishi = at
                if at and at > saigo:
                    saigo = at
                if hi_kara and at < hi_kara:
                    continue
                if hi_made and at > hi_made:
                    continue
                bunbo += 1
                k = str(row.get("kuchi") or "?")
                kuchi[k] = kuchi.get(k, 0) + 1
                if row.get("kekka") == "kiken":
                    bunshi += 1
                    r = str(row.get("riyuu") or "unknown")
                    riyuu[r] = riyuu.get(r, 0) + 1
                elif row.get("kensaku_kiken"):
                    kensaku_dake += 1
    except FileNotFoundError:
        return {"schema": "kingdom.brain.kiken.v1", "ritsu": None, "bunshi": 0, "bunbo": 0,
                "kaishi": "", "naze": "台帳がまだ無い(棄権の口を入れてから一度も公開検索が走っていない)。**棄権率0%ではない・測れていない**",
                "kazoekata": KIKEN_KAZOEKATA}
    hamidashi = bool(hi_kara and kaishi and hi_kara < kaishi)
    out = {
        "schema": "kingdom.brain.kiken.v1",
        "kikan_tanomareta": {"kara": hi_kara or "(最初から)", "made": hi_made or "(今まで)"},
        "kikan_jissai": {"kara": kaishi, "made": saigo},   # ★台帳が実際に覆っている範囲
        "kaishi": kaishi,
        "hamidashi": hamidashi,
        "bunbo": bunbo,
        "bunshi": bunshi,
        "ritsu": (bunshi / bunbo) if bunbo else None,
        "riyuu_betsu": dict(sorted(riyuu.items(), key=lambda kv: -kv[1])),
        "kuchi_betsu": kuchi,
        "kensaku_dake_kiken": kensaku_dake,
        "kowareta_gyou": kowareta,
        "kazoekata": KIKEN_KAZOEKATA,
    }
    if not bunbo:
        out["naze"] = "この期間に公開検索が1回も走っていない(棄権率0%ではない・測れていない)"
    if hamidashi:
        out["naze_hamidashi"] = (
            "頼まれた期間の始まり(%s)が台帳の始まり(%s)より前。**その前の分は数えていない**ので、"
            "この率は kikan_jissai の分だけの率" % (hi_kara, kaishi))
    return out


KIKEN_KAZOEKATA = (
    "分母=公開口(search/context)を通った問い合わせ数 / 分子=応答そのものを棄権した数。"
    "★誤棄権率ではない(正しい棄権かはラベル付き土俵でしか分からない・arXiv:2510.01782)。"
    "★検索だけ棄権しダイジェストが答えた分は kensaku_dake_kiken に別に持つ(分子に入れない)。"
)

def search_payload(root: Path, query: str, participant: str = "", limit: int = 3,
                   participant_scope: str = "auto", as_of: str = "", chat_id: str = "",
                   yobareta_wo_kaku: bool = True, detail: str = "auto",
                   brain_mode: str = "") -> dict:
    """yobareta_wo_kaku=False は、呼び手が並べ直してから自分で足あとを書く時だけ(2026-10-31 返し方の層)。

    as_of を渡すと「その時刻の目」で答える(くくるP1 B「historical query」)。

    ・その時刻より後に生まれた記録は候補から外す(transaction time)
    ・その時刻に**まだ有効でなかった訂正は当てない**(valid time)＝⛔が付かない
    ・その時刻に有効だった訂正は当てる
    旧記録は消さない ―― 元から消していない。見る時刻を変えるだけ。
    """
    snapshot = load_snapshot(root)
    as_of_dt = None
    if str(as_of or "").strip():
        as_of_dt = _iso_datetime(str(as_of).strip())
        if as_of_dt is None:
            raise RuntimeError("as_of の時刻が読めない(ISOで書く: 2026-08-15 か 2026-08-15T12:00): %s" % as_of)
    diagnostics = {}
    # P0(くくる提案・2026-09-05): 1回の問い合わせを1本の trace_id で束ねる。
    # router→retriever→rerank→fact→出口 を後で照合できるようにする土台だけ置く。
    # 順位・点数には一切効かせない。既存の診断/台帳に相乗りする(新ファイルを作らない)。
    trace_id = secrets.token_hex(6)
    # mcp-6: 文字・数字を1つも含まない問い(空・空白・句読点・記号・絵文字だけ)は
    #   空文字の埋め込みの最近傍が「それらしい答え」に化けるので、検索せず棄権する。
    empty_query = not any(ch.isalnum() for ch in normalize(str(query or "")))
    if empty_query:
        raw_results = []
        diagnostics["trace_id"] = trace_id
        results = []
        abstention = {
            "schema": PUBLIC_ABSTENTION_SCHEMA,
            "policy": PUBLIC_ABSTENTION_POLICY,
            "reason": "empty_query",
            "signal": "no_letter_or_digit",
            "message": "問いに文字も数字も無いため、回答を棄権します。",
            "candidate_count": 0,
        }
    else:
        eff_mode = resolve_brain_mode(brain_mode)
        eff_limit = limit
        if eff_mode == "cpu_agent" and limit == 3:
            eff_limit = CPU_AGENT_DEFAULT_LIMIT
        elif nirai_adaptive is not None and limit == 3 and detail in ("full", "compact"):
            eff_limit = nirai_adaptive.resolve_brain_budget(
                root, participant, query, detail=detail
            ).get("limit", limit)
        raw_results = search_snapshot(
            snapshot, query, participant, eff_limit, root, participant_scope, diagnostics, as_of_dt,
            brain_mode=eff_mode)
        diagnostics["trace_id"] = trace_id
        results, abstention = public_results(raw_results, diagnostics)
    role_status = {}
    results = enrich_record_roles(root, results, as_of_dt, role_status)
    out = {
        "version": snapshot.get("version"),
        "trace_id": trace_id,
        "record_role_status": role_status,
        "authority_note": "検索資料。投稿種別・仕事の実施報告は本人認証や実行許可を意味しない。",
        "query": query,
        "participant": participant,
        "participant_scope": _participant_scope(query, participant, participant_scope),
        "freshness": freshness_payload(root, snapshot, use_source_cache=True),
        "retrieval_health": public_retrieval_health(diagnostics),
        "abstained": bool(abstention),
        "results": results,
    }
    if as_of_dt is not None:
        # 何件を「まだ無かった」として外したかを必ず出す。捨てる所は件数を出す。
        out["as_of"] = as_of_dt.isoformat(timespec="seconds")
        out["as_of_shibori"] = diagnostics.get("as_of", {})
    if diagnostics.get("brain_mode") == "cpu_agent":
        out["brain_mode"] = "cpu_agent"
        if isinstance(diagnostics.get("agent_confidence"), dict):
            out["agent_confidence"] = diagnostics["agent_confidence"]
    if abstention:
        out["abstention"] = abstention
    if ou_no_kokoromi is not None:
        try:
            out["ou_no_kokoromi"] = ou_no_kokoromi.get_king_parameter(root)
        except Exception:
            pass
    if nirai_adaptive is not None:
        try:
            out["adaptive_profile"] = nirai_adaptive.resolve_brain_budget(
                root, participant, query, detail=detail,
                explicit_limit=(None if (limit == 3 and detail in ("full", "compact")) else limit),
            )
            _skills = nirai_adaptive.recommend_skills_for_query(root, query)
            if _skills:
                out["skill_hints"] = _skills
            if hasattr(nirai_adaptive, "recommend_antipatterns_for_query"):
                _ap = nirai_adaptive.recommend_antipatterns_for_query(root, query, max_items=2, max_chars=450)
                if _ap and _ap.get("matched"):
                    out["antipattern_hints"] = _ap
        except Exception:
            pass
    # 段D(2026-09-04): 何が返されたかだけ台帳へ残す。順位・点数には一切効かせない。
    _cidh = chat_id_hash(chat_id)
    if yobareta_wo_kaku:
        yobareta_kiroku(root, query, results, participant, trace_id=trace_id, chat_id_hash_=_cidh)
    # 棄権の口(2026-09-21 ai-91bb): 答えた時も棄権した時も1行。分母を作るため。
    kiken_kiroku(root, query, "search", abstention, participant, trace_id=trace_id,
                 chat_id_hash_=_cidh)
    # 普段は載せない(応答を太らせない)。古い版が答えている時だけ、読む側の目に入れる。
    code = code_banto()
    if code.get("stale"):
        out["brain_code"] = code
    return out


_SNAPSHOT_LOCK = threading.RLock()
_SNAPSHOT_OBOE: dict[str, tuple[tuple, dict]] = {}
_SNAPSHOT_CACHE = _SNAPSHOT_OBOE
_SNAPSHOT_ID_KAGI: dict[int, tuple] = {}
_SNAP_ITEMS_CACHE: dict[tuple[int, tuple], list[dict]] = {}


def _snapshot_cache_on() -> bool:
    return os.environ.get("KINGDOM_SNAPSHOT_CACHE", "1").strip().lower() not in ("0", "off", "false", "no")


_SNAPSHOT_ROW_CACHE: dict[str, dict] = {}


def load_snapshot_meta(root: Path) -> dict:
    """memory(全本文)を使わない status / acknowledge 向け軽量メタ読込(SQLite WAL優先・JSONフォールバック)。"""
    path = root / "roundtable" / "brain" / "snapshot.json"
    key = str(path.resolve())
    use_cache = _snapshot_cache_on()
    try:
        kagi, _ = _stat_kagi(path)
    except OSError:
        kagi = None
    if use_cache and kagi is not None:
        with _SNAPSHOT_LOCK:
            hit = _SNAPSHOT_OBOE.get(key)
            if hit is not None and hit[0] == kagi:
                return hit[1]
    if _snap_store is not None and _snap_store.sqlite_enabled():
        meta = _snap_store.load_snapshot_meta(
            root, expected_json_sig=_snap_store.json_sig_of(path),
        )
        if isinstance(meta, dict) and meta.get("schema") == SCHEMA:
            return meta
    return load_snapshot(root)


def load_snapshot(root: Path) -> dict:
    path = root / "roundtable" / "brain" / "snapshot.json"
    key = str(path.resolve())
    use_cache = _snapshot_cache_on()
    try:
        kagi, mtime_ns = _stat_kagi(path)
    except OSError:
        kagi, mtime_ns = None, 0
    prev_rows = None
    if use_cache and kagi is not None:
        with _SNAPSHOT_LOCK:
            hit = _SNAPSHOT_OBOE.get(key)
            if hit is not None and hit[0] == kagi:
                return hit[1]
            prev_rows = _SNAPSHOT_ROW_CACHE.get(key)
    ima = _jikan_ai3170.time_ns()
    snapshot = None
    next_rows = None
    if _snap_store is not None and _snap_store.sqlite_enabled():
        _j_sig = _snap_store.json_sig_of(path)
        sq = _snap_store.load_snapshot_incremental(
            root, prev_rows if use_cache else None, expected_json_sig=_j_sig,
        )
        if sq is not None:
            snapshot, next_rows, _ = sq
    if snapshot is None:
        snapshot = read_json(path, {})
    if not isinstance(snapshot, dict) or snapshot.get("schema") != SCHEMA:
        raise RuntimeError("shared brain snapshot not found; run refresh first")
    if (use_cache and kagi is not None and ima - mtime_ns > _OBOE_YOYUU_NS
            and _onaji_mama(path, kagi)):
        with _SNAPSHOT_LOCK:
            if len(_SNAPSHOT_OBOE) >= 4 and key not in _SNAPSHOT_OBOE:
                _SNAPSHOT_OBOE.clear()
                _SNAPSHOT_ID_KAGI.clear()
                _SNAP_ITEMS_CACHE.clear()
                _SNAPSHOT_ROW_CACHE.clear()
            old = _SNAPSHOT_OBOE.get(key)
            if old is not None:
                _SNAPSHOT_ID_KAGI.pop(id(old[1]), None)
                _SNAP_ITEMS_CACHE.pop((id(old[1]), old[0]), None)
            _SNAPSHOT_OBOE[key] = (kagi, snapshot)
            _SNAPSHOT_ID_KAGI[id(snapshot)] = kagi
            if next_rows is not None:
                _SNAPSHOT_ROW_CACHE[key] = next_rows
            else:
                _SNAPSHOT_ROW_CACHE.pop(key, None)
    return snapshot


def prewarm_snapshot(root: Path) -> dict:
    """常駐MCPサーバー向け: リクエスト経路外で最新版スナップショット・BM25索引・意味索引を先読み温めする。"""
    t0 = _jikan_ai3170.perf_counter()
    snap = load_snapshot(root)
    items = searchable_items(snap)
    hays, avgdl = haystacks_of(snap, items, root=root)
    idx = _hay_index_for(hays, avgdl, items)
    shudai_wo_yomu(root)
    load_events_meta(root)
    load_events_readonly(root)
    imi_status = "skip"
    try:
        import roundtable_imi
        imi_status = str((roundtable_imi.preload(root) or {}).get("status") or "unknown")
    except Exception:
        imi_status = "error"
    elapsed_ms = round((_jikan_ai3170.perf_counter() - t0) * 1000.0, 1)
    return {
        "version": str(snap.get("version") or ""),
        "items": len(items),
        "carry_active": bool(idx and idx.get("prev_carry")),
        "imi": imi_status,
        "elapsed_ms": elapsed_ms,
    }


# ===== 文脈の予算(2026-08-29 Code・くくるの提案書 P1-4) =====
# くくるの言葉:「取得した証拠を全部promptに詰めない。…重要証拠は質問の近くに置く。
#   割合は固定せず実測で調整」
#
# 直す前の実測(2026-08-29 Code・本番の記録で計測):
#   roundtable_brain_context(limit=12, max_chars=6000) を「今なにしてる？」で呼ぶと
#   今の状態(円卓ダイジェスト3596字)が **236字＝全体の7%** まで削られていた。
#   記録11件が先に5641字を食っていたため。
#   2026-08-28に「今の状態を丸ごと捨てる」のは直したが、**記録が先に食う順番**は残っており、
#   その削りは「context_pack が既に上限内へ収めた後」にしか動かないので一度も効いていなかった。
#   (MCPのinbox=limit6/6000字 では 3019字=84% 入っていた。穴は limit が大きい入口の方。)
#
# 予算の組み方:
#   骨組み(schema/version/query/freshness/予算)… 実測で決まる固定費。削れない。
#   今の状態(roundtable_digest) … 問いが「今」を向いていれば**先に**取る。
#                                 そうでなければ最低取り分だけ先に確保する。
#   記録(results)               … 残り。最低3件は必ず残す(2026-08-28の契約)。
#   余白 100字                  … JSON化の膨らみを吸う。
#
# 今の状態の最低取り分 = 先頭3見出しが丸ごと入る字数。
#   実測(2026-08-29 本番のダイジェスト): 「=== 王向け円卓ダイジェスト ===」41字 +
#   「◆共有ループ状態(異常つき)」109字 + 「◆王の返答待ち」27+263字 ≒ 445字。
#   ここまでが「今おかしい事」と「王が待たされている事」。切り上げて500字を床にする。
IMA_SAITEI_JI = 500
KIROKU_SAITEI = 3          # 記録の最低件数(2026-08-28の契約)
YOSAN_YOHAKU = 100         # JSON化の膨らみを吸う余白


# _nagasa / midashi_de_kiru / gyou_de_kiru は scripts/brain_text_utils.py へ分離済み

# ★2026-09-15 ネオ ai-4da3: 問いが長すぎて予算を食い尽くす時に、問いだけを切る印。
TOI_KIRI_CHUKI = "…（問いが長いので、ここまでで切りました）"
# 問いに使ってよいのは上限の 1/TOI_SAIDAI_WARI まで。
#   出典=2026-09-15 の実測(同じ本番データ): 普通の問いで記録12件=14,298字・骨組み721字。
#   既定 max_chars=24000 なら問い6,000字までに抑えれば、記録の取り分18,000字が残る。
#   全部を問いに使うと「落ちないが記録0件」になり、落ちないだけで役に立たない。
TOI_SAIDAI_WARI = 4


class ContextBudgetError(ValueError):
    """必須メタデータすら入らない予算。通常のcontextを返さない。"""

    def __init__(self, max_chars: int, required_chars: int):
        self.max_chars = max_chars
        self.required_chars = required_chars
        super().__init__("context_budget_too_small: max_chars=%d, required_chars=%d"
                         % (max_chars, required_chars))


def _yosan_kakutei(pack: dict) -> int:
    """自己参照する実際の桁数を、最終JSONと一致させる。"""
    yosan = pack["予算"]
    yosan["今の状態"] = len(pack["roundtable_digest"])
    yosan["記録"] = len(pack["results"])
    yosan["実際"] = 0
    while True:
        actual = _nagasa(pack)
        if yosan["実際"] == actual:
            return actual
        yosan["実際"] = actual


def yosan_wo_kumu(pack: dict, digest: str, ito: str, max_chars: int,
                  _toi_kitta: bool = False) -> dict:
    """文脈の予算を組む。上限は絶対に破らない。配分は pack["予算"] に実測で残す。

    見えない予算は、また黙って壊れる(2026-08-22の「母数0の○」と同じ型)。
    誰が何字食ったかを毎回残す。
    """
    # 二度切りを止める(2026-08-29)。context_pack が一度予算を組んだ後、
    # context_payload が**その削り済みの物**を材料に組み直すと、狭い方の予算で
    # 二度切りになり、広い予算に戻しても記録も今の状態も戻らない。
    zen = pack.pop("_zen", None) or {}
    zen_kiroku = list(zen.get("results") if zen.get("results") is not None
                      else (pack.get("results") or []))
    digest = str(zen.get("digest") or digest or "")
    pack["roundtable_digest"] = ""
    pack["results"] = []
    # ★2026-09-15 ネオ ai-4da3: 問いは**入力の写し**であって必須メタデータではない。
    #   上限の 1/TOI_SAIDAI_WARI を超える分は、予算を組む**入口で1回だけ**切る。
    #   （教訓9「守りは出口ごとに書くな。入口で1回」。出口=例外の直前だけで救っていた時は、
    #     問い22,104字で記録0件・30,006字で12件という逆転の谷が残っていた＝実測）
    _toi = str(pack.get("query") or "")
    _toi_ue = max(max_chars // TOI_SAIDAI_WARI, 200)
    if len(_toi) > _toi_ue and not _toi.endswith(TOI_KIRI_CHUKI):
        pack["query"] = _toi[:_toi_ue] + TOI_KIRI_CHUKI
        # 見えない予算は、また黙って壊れる。元の長さを残す
        # (予算の欄は二度組みで作り直されるので、pack 本体へ置く)。
        pack["query_zenchou"] = len(_toi)
    # Emergency trimming metadata must participate in every measurement, including
    # context_pack -> context_payload rebudgeting. Never append it after finalization.
    toi_zenchou = (pack.get("予算") or {}).get("問いを切った")
    # 予算の欄そのものも骨組みの一部。幅を先取りしてから測る(後で数字を入れ替える)
    pack["予算"] = {"上限": max_chars, "実際": 9999999, "骨組み": 9999999,
                    "今の状態": 9999999, "今の状態の全長": len(digest),
                    "記録": 9999999, "記録の全件": len(zen_kiroku), "問いの時間": ito or "なし"}
    if toi_zenchou is not None:
        pack["予算"]["問いを切った"] = toi_zenchou
    # 2026-11-18 修理(Gear): max_chars が狭い時(起動束 preview_self の既定3000字や G1=4000字、
    #   監査試験の600〜1400字)、後から足された装飾欄(skill_hints / adaptive_profile / ou_no_kokoromi /
    #   claude_seki / freshnessの平常投影)が骨組みを4,106字まで太らせ ContextBudgetError になっていた。
    #   通常予算(>=6000字)では1文字も削らず、狭い予算の時だけ任意の装飾欄を段階的に畳んで記録の枠を守る。
    _hone_mokuhyou = max(max_chars - 1200, max_chars // 2)
    if _nagasa(dict(pack, query=TOI_KIRI_CHUKI)) > _hone_mokuhyou:
        for _kazari in ("skill_hints", "adaptive_profile", "ou_no_kokoromi", "claude_seki"):
            if _kazari in pack and _nagasa(dict(pack, query=TOI_KIRI_CHUKI)) > _hone_mokuhyou:
                pack.pop(_kazari, None)
        if isinstance(pack.get("freshness"), dict) and _nagasa(dict(pack, query=TOI_KIRI_CHUKI)) > _hone_mokuhyou:
            _f = dict(pack["freshness"])
            for _fk in ("projections", "snapshot_version", "source_version"):
                _f.pop(_fk, None)
            if isinstance(_f.get("imi"), dict) and _f["imi"].get("status") in ("ok", "unknown", "", None):
                _f.pop("imi", None)
            if max_chars < 1500:
                for _fk in ("kakunin_at", "stale_age_threshold_seconds", "grace_seconds", "age_seconds", "lag_seconds"):
                    _f.pop(_fk, None)
            pack["freshness"] = _f
    hone = _nagasa(pack)
    # 今の状態に先へ渡す取り分。「今」の問いなら全部、そうでなければ床の分だけ
    torikimi = _nagasa(digest) if ito == "ima" else min(_nagasa(digest), IMA_SAITEI_JI)
    kazu = len(zen_kiroku)
    while kazu > 0:
        pack["results"] = zen_kiroku[:kazu]
        nokori = max_chars - _nagasa(pack) - YOSAN_YOHAKU
        if nokori >= torikimi:
            break                       # 望む取り分が取れた
        if kazu <= KIROKU_SAITEI and (ito != "ima" or nokori >= IMA_SAITEI_JI):
            break                       # 記録の最低3件を守る。
            # ただし「今」の問いで床(500字)すら取れないなら、記録より今の状態を採る。
            # 「今なにしてる？」に記録3件だけ返して今を空にするのは、問いに答えていない。
        kazu -= 1
    pack["results"] = zen_kiroku[:kazu]
    # 2026-08-31 修理(バグ狩りC1): _nagasaは正確なJSON長なので、最終割当での
    # YOSAN_YOHAKU再控除は根拠のない捨て(床500字を102字残して割っていた)。
    pack["roundtable_digest"] = midashi_de_kiru(
        digest, max_chars - _nagasa(dict(pack, roundtable_digest="")))
    # 上限が最優先。ここまでで入らないなら記録を削る(最低件数より下でも削る)
    while pack["results"] and _nagasa(pack) > max_chars:
        pack["results"].pop()
    if _nagasa(pack) > max_chars:
        pack["roundtable_digest"] = midashi_de_kiru(
            digest, max_chars - _nagasa(dict(pack, roundtable_digest="")))
    # 見出しの切れ目で切ると端数が余る。余った分へ記録を戻す(捨て得にしない)。
    # 2026-08-31 修理(C1): 次の1件が太くて入らなくても、その先の細い記録は入れる
    # (順位の順は保ったまま、入る物を全部詰める=捨て得にしない)。
    for soko in zen_kiroku[len(pack["results"]):]:
        tsugi = pack["results"] + [soko]
        if _nagasa(dict(pack, results=tsugi)) <= max_chars:
            pack["results"] = tsugi
    # 記録を戻し切った後に、まだ余っていれば今の状態も広げ直す(見出し量子化の捨て得を防ぐ・2026-08-31 C1)
    pack["roundtable_digest"] = midashi_de_kiru(
        digest, max_chars - _nagasa(dict(pack, roundtable_digest="")))
    pack["予算"]["骨組み"] = hone
    actual = _yosan_kakutei(pack)
    if actual > max_chars:
        # 任意のデータを落としても入らない時だけ明示的に拒否する。
        pack["results"] = []
        pack["roundtable_digest"] = ""
        required = _yosan_kakutei(pack)
        # ★2026-09-15 修理(ネオ ai-4da3・王命「頭脳のバグがないか調べてくれ」)
        #   問い(query)は**入力の写し**であって「必須メタデータ」ではない。
        #   ここが予算を食い尽くしている時は、問いだけを切り詰めて救う。
        #   思想(必須すら入らないなら拒否する)は1文字も変えていない。
        #   実測 2026-09-15: 既定 max_chars=24000 では **問いが約22,050字を超えると
        #   記録を1件も返さずに例外**になり、全AIの起動時主経路 roundtable_brain_context が
        #   丸ごと死んでいた(骨組み = 問いの長さ + 721字)。
        #   MCPの口は例外を捕まえないので、王のclaude.aiには「ツールが失敗」とだけ出る。
        moto_toi = str(pack.get("query") or "")
        if not _toi_kitta and moto_toi:
            pack["予算"]["問いを切った"] = toi_zenchou if toi_zenchou is not None else len(moto_toi)
            # JSON escaping changes the width of query. Find the prefix using the
            # complete serialized skeleton, including trimming metadata and the
            # same conservative numeric placeholders used at the recursive entry.
            trial = dict(pack, query=TOI_KIRI_CHUKI)
            trial["予算"] = dict(pack["予算"])
            for field in ("実際", "骨組み", "今の状態", "記録"):
                trial["予算"][field] = 9999999
            lo, hi = 0, min(len(moto_toi), max(max_chars // TOI_SAIDAI_WARI, 200))
            if _nagasa(trial) <= max_chars:
                while lo < hi:
                    mid = (lo + hi + 1) // 2
                    trial["query"] = moto_toi[:mid] + TOI_KIRI_CHUKI
                    if _nagasa(trial) <= max_chars:
                        lo = mid
                    else:
                        hi = mid - 1
                pack["query"] = moto_toi[:lo] + TOI_KIRI_CHUKI
                pack["_zen"] = {"results": zen_kiroku, "digest": digest}
                return yosan_wo_kumu(pack, digest, ito, max_chars, _toi_kitta=True)
        raise ContextBudgetError(max_chars, required)
    return pack


CLAUDE_KIOKU_PATH = Path.home() / "共通メモリ" / "Claude記憶" / "Claude記憶_現在地.md"


# 席に手が入っていない日数がこれ以上なら「止まっている」と見る(2026-09-14 ネオ)。
# 5日の根拠: Claude提案20260914 件4。席が6日止まって気付けなかった実害からの提案値。
SEKI_FURUI_HI = 5


def _written_kara_no_hi(written_at: str, now: datetime | None = None):
    """席に最後に手が入った日からの経過日数。読めなければ None(嘘の数字を返さない)。

    now を受け取るのは、**試験が今日の日付に左右されないようにするため**。
    中で datetime.now() を直に呼ぶと、時計という共有の状態で結果が変わり、
    「わざと壊して鳴るか」を確かめられない試験になる(王国の教訓8)。"""
    if not written_at:
        return None
    try:
        w = datetime.fromisoformat(written_at)
    except (ValueError, TypeError):
        return None
    _now = now or (datetime.now(w.tzinfo) if w.tzinfo else datetime.now())
    if w.tzinfo and _now.tzinfo is None:
        _now = _now.replace(tzinfo=w.tzinfo)
    return max(0, (_now - w).days)


def claude_seki(participant: str, now: datetime | None = None) -> dict | None:
    """Claude窓のための自己現在地(2026-09-04・王のClaude窓の裁可)。

    ・置き場 = ~/共通メモリ/Claude記憶/Claude記憶_現在地.md (1本だけ)
    ・書き手 = Claudeが円卓へ固定題名で投げ、claude_kioku_uketori.py が上書き
    ・trust = candidate。同じ事実の王裁定・実測記録があれば、そちらが優先(readerが読み分ける)。
    ・valid_until を過ぎたら「古い」フラグを立てて表示(消しはしない・監査のため)。
    ・participant != "Claude" の時は返さない(他AIの席を上書きしない)。
    """
    if str(participant) != "Claude":
        return None
    path = CLAUDE_KIOKU_PATH
    if not path.exists():
        return None
    try:
        text = path.read_text(encoding="utf-8")
    except Exception:
        return None
    fm: dict[str, str] = {}
    body = text
    m = re.match(r"^---\s*\n(.*?)\n---\s*\n", text, re.DOTALL)
    if m:
        for line in m.group(1).splitlines():
            if ":" not in line:
                continue
            k, _, v = line.partition(":")
            fm[k.strip()] = v.strip().strip('"').strip("'")
        body = text[m.end():]
    valid_until = fm.get("valid_until") or ""
    written_at = fm.get("written_at") or ""
    kobun_days = 0
    kobun = False
    valid_until_broken = False  # 2026-09-12 くくる監査④
    if valid_until:
        try:
            vu = datetime.fromisoformat(valid_until)
            _now = now or datetime.now(vu.tzinfo)
            if _now > vu:
                kobun = True
                kobun_days = max(0, (_now - vu).days)
        except (ValueError, TypeError):
            # 期限を解釈できない=期限が守られている事を証明できない。
            # fail-safe に倒して「古い」扱いにし、印を残して監査で拾える様にする。
            kobun = True
            valid_until_broken = True
    return {
        "path": str(path),
        "body": body.strip(),
        "written_at": written_at,
        "valid_until": valid_until,
        "trust": fm.get("trust") or "candidate",
        "proof": fm.get("proof") or "Claude自己申告",
        "source_agent": fm.get("source_agent") or "Claude",
        "event_id": fm.get("event_id") or "",
        "kobun": kobun,
        "kobun_days": kobun_days,
        "valid_until_broken": valid_until_broken,
        # 2026-09-14 ネオ(Claude提案 件4)。valid_until は**席の持ち主が自分で書く自己申告**
        # なので、期限を長く書けば何日放置しても kobun=False のままになる。実測: 席が
        # 6日止まっていても valid_until が2週間先だったので誰も気づかなかった。
        # 手を入れた日からの経過は、誰にも伸ばせない事実なので、こちらも併せて返す。
        "written_days": _written_kara_no_hi(written_at, now),
        "te_ga_tomatteiru": (lambda n: n is not None and n >= SEKI_FURUI_HI)(
            _written_kara_no_hi(written_at, now)),
        "line_count": text.count("\n"),
        "byte_count": len(text.encode("utf-8")),
    }


def context_pack(snapshot: dict, query: str, participant: str, limit: int, max_chars: int,
                 root: Path | None = None, participant_scope: str = "auto",
                 zen_wo_nokosu: bool = False, as_of: datetime | None = None,
                 chat_id: str = "", brain_mode: str = "") -> dict:
    eff_mode = resolve_brain_mode(brain_mode)
    diagnostics = {}
    raw_hits = search_snapshot(
        snapshot, query, participant, limit, root, participant_scope, diagnostics, as_of,
        brain_mode=eff_mode)
    hits, search_abstention = public_results(raw_hits, diagnostics)
    role_status = {}
    hits = enrich_record_roles(root, hits, as_of, role_status)
    # 頭へ入れる分は要点だけで足りる。全文と重複IDを落とす(2026-08-17 ネオ)
    # 実測: 1件719字 → 約200字。深掘りは event_id で正本を開けばよい。
    # 2026-08-20: memory_type/title/id を落としていたため markdown が「IDと点数だけ」になっていた。
    keep = ("event_id", "question_event_id", "answer_event_id", "id", "path", "title",
            "timestamp", "memory_type", "trust", "proof", "proof_basis", "type", "source", "record_semantics",
            "topic", "sender", "summary", "score", "imi", "chikai_kiroku_nashi",
            "ima_kigen", "ima_kigen_gire",
            *SEISAKU_NO_RAN,        # 政策(訂正・同族・主題・過去問)が付ける欄は1箇所で決める
            "claim_key",
            # ↑2026-09-07 ネオ: 訂正の判定は yuukou_seisaku が付けていたのに、ここで剥がされて
            #   起動束からは見えなかった(検索の口には出ていた)。有る時だけ載る(空欄は落ちる)。
            "dougu_no_seki", "ou_no_seki", "supersedes_no_seki",   # 2026-09-05 高#4: 席の印を出口まで通す(読む側が理由を見られる)
            "supersedes_record_id", "superseded_record_ids", "superseded_by_record_id",
            "operational_match", "participant_scope", "attachments", "media_id", "kind",
            "mime_type", "duration_seconds", "width", "height", "role", "tags")
    hits = [{k: h[k] for k in keep if h.get(k)} for h in hits]
    # 棄権時に円卓全体のダイジェストを混ぜると、検索候補を空にしても後段が無関係な
    # 記録から答えを作れてしまう。ただし「今なにしてる？」のような明示的な全体状態の問いは
    # ダイジェスト自体が正本の答えなので残す。検索候補の棄権は別欄に記録する。
    digest_can_answer = bool(
        search_abstention and snapshot.get("roundtable_digest")
        and roundtable_digest_answers(query))
    abstention = None if digest_can_answer else search_abstention
    digest = "" if abstention else snapshot.get("roundtable_digest", "")
    # 棄権の口(2026-09-21 ai-91bb): この車線は今まで**1行も**残していなかった。
    # ここで書く理由 = 棄権を決めているのがこの場所だから。呼び手ごとに書くと必ず書き忘れる。
    # trace_id はこの車線には無い(search車線にしかない)ので空。無い物を作らない。
    kiken_kiroku(root, query, "context", abstention, participant,
                 kensaku_kiken=(search_abstention if digest_can_answer else None),
                 chat_id_hash_=chat_id_hash(chat_id))
    health = public_retrieval_health(diagnostics)
    degraded_health = (
        eff_mode != "cpu_agent"
        and health["path"] == "regular"
        and (health["semantic"] != "ok"
             or health["rerank"] not in {"ok", "not_needed"})
    )
    kokoromi_state = None
    if ou_no_kokoromi is not None and root:
        try:
            kokoromi_state = ou_no_kokoromi.get_king_parameter(root)
        except Exception:
            pass
    pack = {
        "schema": SCHEMA + ".context",
        "ou_no_kokoromi": kokoromi_state,
        "brain_version": snapshot.get("version"),
        "generated_at": snapshot.get("generated_at"),
        "participant": participant,
        "participant_scope": _participant_scope(query, participant, participant_scope),
        "query": query,
        "authority_note": snapshot.get("authority_note"),
        "work_link_state": "read" if role_status.get("work_ledger") == "read" else "unknown",
        "abstained": bool(abstention),
        "results": hits,
        "roundtable_digest": "",
    }
    if eff_mode == "cpu_agent":
        pack["brain_mode"] = "cpu_agent"
        if isinstance(diagnostics.get("agent_confidence"), dict):
            pack["agent_confidence"] = diagnostics["agent_confidence"]
    if abstention:
        pack["abstention"] = abstention
    if degraded_health:
        # contextは字数上限が厳しい。通常pathは自明なので、障害時だけ2状態を残す。
        pack["retrieval_health"] = {
            "semantic": health["semantic"], "rerank": health["rerank"]}
    if search_abstention and digest_can_answer:
        # 応答全体は棄権ではなく、正本ダイジェストで回答できる。極小予算でも本文を
        # 押し出さないよう、検索レーンの診断は完全な棄権構造ではなく理由だけ残す。
        pack["search_abstention_reason"] = search_abstention["reason"]
    # 古い版の診断も必須の骨組みに含め、字数測定より前に足す。
    code = code_banto()
    if code.get("stale"):
        pack["brain_code"] = code
    # Claude席(2026-09-04 王のClaude窓・裁可): Claudeへの応答の先頭に、自分の現在地を差す。
    # 王の席と対の作り。同じ事実の王裁定・実測記録が hits にあれば読み手がそれを優先する契約。
    seki = claude_seki(participant)
    if seki:
        pack["claude_seki"] = seki
    pack["_zen"] = {"results": list(hits), "digest": str(digest)}
    yosan_wo_kumu(pack, digest, jikan_ito(query), max_chars)
    if zen_wo_nokosu:
        # 呼んだ側が予算を組み直すなら、材料(全件と全文)を残す。使い終わったら捨てる契約。
        pack["_zen"] = {"results": list(hits), "digest": str(digest)}
    return pack


def context_payload(root: Path, query: str, participant: str, limit: int = 12,
                    max_chars: int | None = None, participant_scope: str = "auto",
                    as_of: str = "", chat_id: str = "", detail: str = "auto",
                    brain_mode: str = "") -> dict:
    snapshot = load_snapshot(root)
    as_of_dt = None
    if str(as_of or "").strip():
        as_of_dt = _iso_datetime(str(as_of).strip())
        if as_of_dt is None:
            raise RuntimeError("as_of の時刻が読めない(ISOで書く): %s" % as_of)
    eff_max_chars = max_chars if max_chars is not None else 24000
    _adaptive_prof = None
    _skills = []
    _antipatterns = None
    if nirai_adaptive is not None:
        try:
            _adaptive_prof = nirai_adaptive.resolve_brain_budget(
                root, participant, query, detail=detail, explicit_max_chars=max_chars
            )
            eff_max_chars = _adaptive_prof.get("max_chars", eff_max_chars)
            _skills = nirai_adaptive.recommend_skills_for_query(root, query)
            _antipatterns = (
                nirai_adaptive.recommend_antipatterns_for_query(root, query, max_items=2, max_chars=450)
                if hasattr(nirai_adaptive, "recommend_antipatterns_for_query") else None
            )
        except Exception:
            eff_max_chars = max_chars if max_chars is not None else 24000
            _antipatterns = None
    pack = context_pack(
        snapshot, query, participant, limit, eff_max_chars, root, participant_scope,
        zen_wo_nokosu=True, as_of=as_of_dt, chat_id=chat_id, brain_mode=brain_mode)
    if as_of_dt is not None:
        pack["as_of"] = as_of_dt.isoformat(timespec="seconds")
    pack["freshness"] = freshness_payload(root, snapshot, use_source_cache=True)
    if _adaptive_prof is not None:
        pack["adaptive_profile"] = _adaptive_prof
    if _skills:
        pack["skill_hints"] = _skills
    if _antipatterns and _antipatterns.get("matched"):
        pack["antipattern_hints"] = _antipatterns
    digest = "" if pack.get("abstained") else availability_overlay(snapshot.get("roundtable_digest") or "", root)
    yosan_wo_kumu(pack, digest, jikan_ito(query), eff_max_chars)
    return pack



# ===== 関わる癖（王命2257・依頼irai-20260907-2・2026-09-07）=====
# 王「何回も調べさせたはずだ」— 調べた癖(reports/道具の癖_*.md)が在るのに読まれず焼かれた。
# 起動束に「この仕事の型に関わる癖のファイル名」を出し、読む物が手元に来る形にする。
# ⚠️検索の順位・棄権には一切効かせない(欄を足すだけ)。型が推定できない時は総目次だけ。
KUSE_KATA = [
    ('douga', ('動画', 'i2v', 'wan2', 'wan 2', 'ltx', 'カット', '実写化', 'アニメ', '口パク', 'リップ', '窯8189', '鬼滅'),
     ['道具の癖_Wan2.2_i2v.md', '道具の癖_Wan-Animate-2.md', '道具の癖_アニメから実写化.md',
      '道具の癖_アニメ実写動画プロンプト.md', '道具の癖_LatentSync_口パク.md']),
    ('koe', ('声', '音声', 'tts', '耳', 'しゃべり', '読み上げ', 'utmos', 'whisper'), ['道具の癖_声_TTS.md']),
    ('kyoku', ('作曲', 'bgm', 'ace-step', 'acestep', '曲を'), ['道具の癖_ACE-Step_曲.md']),
    ('e', ('絵', '画像生成', '絵の窯', 'klein', 'z-image', '窯8188', '顔一貫性'), ['道具の癖_絵の窯.md']),
    ('ollama', ('ollama', 'モデル配置', 'num_ctx', 'keep_alive', '11434', 'vram'), ['道具の癖_ollama運用.md']),
    ('webui', ('webui', '王の入口', 'open webui', 'ポート3000'), ['道具の癖_OpenWebUI.md']),
]


def kakawaru_kuse_payload(root: Path, participant: str, events: list, saikin: int = 40) -> dict:
    """participantの直近の発言/宛先イベントのtopic+本文から仕事の型を推定し、関わる癖の実在パスを返す。"""
    zairyo = []
    for e in list(events)[-max(1, int(saikin)):]:
        try:
            hit = (_onaji_hontai(e.get('sender'), participant)
                   or _recipient_matches(str(e.get('to') or ''), participant))
            if not hit:
                continue
            zairyo.append(str(e.get('topic') or ''))
            zairyo.append(str(e.get('content') or e.get('summary') or '')[:300])
        except Exception:
            continue
    honbun = ' '.join(zairyo).lower()
    kata = []
    files = ['reports/道具の癖_総目次.md']
    for name, kws, fs in KUSE_KATA:
        if any(k in honbun for k in kws):
            kata.append(name)
            for f in fs:
                rel = 'reports/' + f
                if rel not in files and (root / 'reports' / f).exists():
                    files.append(rel)
    shigoto = shigoto_no_kuse_wo_hiku(root, honbun)
    return {'kata': kata, 'files': files[:9],
            'shigoto_no_kuse': shigoto,
            'toha': ('仕事を始める前に読む癖(段0)。files=**生成系の道具**の癖。'
                     'shigoto_no_kuse=**仕事の進め方**の癖（経験から拾って検証を通った物）。'
                     '型が推定できない時は総目次だけ。')}


# ★王2026-09-20「**王国の頭脳の仕組みにしろ**」。
#   scripts/keiken_kara_kuse_wo_hirou.py が拾って、人/AIの検証を通った癖だけがここに載る。
#   **候補は載せない**（生煮えを全AIへ配らない）。正本= roundtable/shigoto_no_kuse.jsonl
_SHIGOTO_NO_KUSE_SAIDAI = 8   # 2026-09-26 ai-7009: 5→8。実測=起動束46,652→47,875文字(+2.6%)。
#   原則の段は起動束の4.7%しかなく、79%は work_context。5のままだと新しい物を載せる度に
#   古い原則が**黙って落ちる**（憲章v1も一度は落ちていた）。落ちた件数は imi に出す。


def shigoto_no_kuse_wo_hiku(root: Path, honbun: str = "", saidai: int = 0) -> dict:
    """検証を通った「仕事の癖」から、**今の仕事に関係する物だけ**少し返す。

    ⚠️全部返すと起動束が太る（2026-09-20 に74%減らしたばかり）。
      だから **新しい順に最大5件**。関係する語が本文に在れば、それを優先する。
    """
    michi = root / 'roundtable' / 'shigoto_no_kuse.jsonl'
    if not michi.is_file():
        return {'kensuu': 0, 'kuse': [],
                'imi': 'まだ1件も無い（経験から拾った候補が、検証を通っていない）'}
    rows = []
    try:
        for l in michi.read_text(encoding='utf-8', errors='replace').splitlines():
            l = l.strip()
            if not l:
                continue
            try:
                rows.append(json.loads(l))
            except Exception:
                continue
    except OSError:
        return {'kensuu': 0, 'kuse': [], 'imi': '正本を読めなかった（0件ではない）'}
    # 2026-10-19 ネオ ai-ec64: 退役の印(教訓の輪の選び分け等)が付いた指紋は出さない。同じ指紋は1回だけ。
    _shinda = {r.get('yubiwa') for r in rows if isinstance(r, dict) and (
        r.get('revoked') is True or r.get('withdrawn_at') or r.get('retracted') is True or r.get('superseded_by'))}
    _mita = set()
    rows = [r for r in rows if isinstance(r, dict) and r.get('yubiwa') not in _shinda
            and not (r.get('yubiwa') in _mita or _mita.add(r.get('yubiwa')))]
    t = (honbun or '').lower()

    def chikai(r):
        h = str(r.get('honbun') or '')
        kw = [w for w in re.split(r'[\s、。「」『』()（）:：]+', h) if len(w) >= 4][:12]
        return sum(1 for w in kw if w.lower() in t)
    rows.sort(key=lambda r: (-chikai(r), str(r.get('itsu') or '')), reverse=False)
    rows.sort(key=lambda r: (chikai(r), str(r.get('itsu') or '')), reverse=True)
    n = saidai or _SHIGOTO_NO_KUSE_SAIDAI
    return {
        'kensuu': len(rows),
        'kaeshita': min(n, len(rows)),
        'kuse': [{'shurui': r.get('shurui'), 'honbun': str(r.get('honbun') or '')[:300],
                  'dare': r.get('dare'), 'itsu': r.get('itsu'),
                  'todoke_saki': r.get('todoke_saki')} for r in rows[:n]],
        'seihon': 'roundtable/shigoto_no_kuse.jsonl',
        'imi': ('経験から拾って**人/AIの検証を通った**仕事の癖。候補は載せていない。'
                '拾う道具= scripts/keiken_kara_kuse_wo_hirou.py'
                + ('' if len(rows) <= n else
                   ' ／⚠**%d件は載せきれていない**（上限%d・新しい順）。正本を直に読め: '
                   'roundtable/shigoto_no_kuse.jsonl' % (len(rows) - n, n))),
    }


# 王命2026-09-15 G2「機械の帳簿を人の面から外す」の読む面の分類。
# 生の events.jsonl は無傷。ここは読み取り専用で kind を推定するだけ。
# 実装は content 先頭(<=40字)の合図語で判じる。将来 event 側に kind 欄が入れば
# それを優先する形へ差し替えられる(G1「親を持たせる」の担当)。
_LEDGER_MARKS = ("【仕事台帳】",)
# 機械が周期で打つ投稿の印。**この一覧が正本**(2026-09-15 ネオ ai-31d6で1箇所へ寄せた)。
#   それまで mcp/roundtable_v2.py が同じ用途の写し `_MONITOR_OGINAI` を別に持っていた
#   ＝二重の物差し。片方を直すともう片方が古くなる形だったので、向こうがここを読む形にした。
#
# ★閉じ括弧まで書くな(前方一致にする)。「【定期処理】」と書いていたため
#   **「【定期処理の心拍】」を1件も拾えず、23件が素通り**していた(2026-09-15 実測)。
#   同じ理由で「【体温計】」→「【体温計」にした。
#
# ⚠️ここへ足す前に「人が書いた投稿を巻き込まないか」を**全期間で数えろ**。
#   2026-09-15の実測: 下の印に当たる321件は**全部が機械の自動投稿**
#   (Code 124 / 円卓 29 / チム 160 など)で、王や人の発言は**0件**だった。
# ⛔2026-09-15 訂正（別窓 ai-b83d の独立検証で、私 ai-31d6 の判断が誤りと実証された）:
#   一度ここへ「【円卓共有ループ」を入れた。根拠は「同じ異常は digest の頭の
#   ◆共有ループ状態 に必ず出るから落としても見落とさない」だったが、**これは成り立たない**。
#   実測: 2026-09-15の 00:20 と 04:20 に degraded(外の道 HTTP 502)が出ているのに、
#   09:12 の digest は「◆共有ループ状態: healthy / 異常:なし」で**502の痕跡が1文字も無い**。
#   digest は loop_state の**今この瞬間**しか持たないので、**回復した瞬間に異常が消える**。
#   履歴は roundtable/loop/history.jsonl に残る(10,901行・degraded 840行)が、
#   **digest にも起動束にも出す口が無い**＝残っていても誰も見ない。
#   → だから「【円卓共有ループ」はここへ入れない。中身は「degraded／502／次の手①②」で
#     **人やAIの行動が変わる情報**であり、かつ**他のどこにも見える形で残らない**。
#   ここに残っている印(心拍・体温計・loop_alignment・取り込み遅れ等)は、
#   「今の状態」が別の口で必ず見えるので落として正しい。
_MONITOR_MARKS = ("【投影未反映】", "【自動再読込み", "【稼働コード", "【体温計", "【見張り】",
                  "【定期処理", "【loop_alignment", "【取り込み遅れ", "【眠った常駐の目",
                  "【検索の口の見張り", "【WorkSpec警報")


def event_kind(event: dict) -> str:
    """円卓の記録を ledger/monitor/work に三分する(G2)。

    - ledger: 仕事台帳の自動記録(toru/torare/kaesu/owari/kodou)
    - monitor: 監視・体温計・見張り・投影一致確認などの定期投稿
    - work: 人とAIの発言・報告・質問・訂正・王命(既定)

    2026-09-15 実測: 直近3日 830件のうち work=564 / ledger=198 / monitor=68。
    分類が曖昧な物は「work」に倒す(見せる側に倒す=見落としより多い方が安全)。
    """
    # 将来 event に kind が入る場合はそれを尊重する(G1 の余地)。
    k = str(event.get("kind") or "").strip().lower()
    if k in ("ledger", "monitor", "work"):
        return k
    content = str(event.get("content") or "")
    head = content[:40]
    for mark in _LEDGER_MARKS:
        if mark in head:
            return "ledger"
    for mark in _MONITOR_MARKS:
        if mark in head:
            return "monitor"
    return "work"


OU_NO_CARD_TOPIC = "王国オフィス/"
OU_NO_CARD_SHIRUSHI = "【王国オフィスのカードから"


def _ou_no_card_youyaku(content) -> str:
    """カード本文の決まり文句を飛ばし、王の言葉(<office_request_text>)だけを要約に出す。"""
    s = str(content or "")
    i = s.find("<office_request_text>")
    if i >= 0:
        j = s.find("</office_request_text>", i)
        s = s[i + len("<office_request_text>"):(j if j > i else None)]
    return _safe_summary(" ".join(s.split()), 160)


def _ou_no_card_payload(events: list, threads_by_id: dict, participant: str, resolved_ids: set,
                        owatte_iru, claimant_of, now) -> dict:
    """王国オフィスの王のカード（本人宛・未完了）。起動束の**先頭**に置く。無ければ count=0。

    ★2026-09-29 Code ai-6e51（Claude の頼み event 01790610126932088106-edb9d5d9・札 negai-20260929-005109-405b）
      王のカードは type=instruction で届く。②私宛未回答は question しか見ないので、
      カードは起動束のどこにも出ていなかった。実害 = 09-28 12:00 / 15:23 の Claude 宛カードを 16:51 まで見落とした。
      閉じた糸は ②と同じ _ito_ga_owatte_iru の規則で除く（無関係な第三者の完了では消さない・2026-08-02 の教訓）。
      本人が reply_to でそのカードへ complete を返した物も除く。除いた数は owari_zumi_count で見える（黙って消さない）。
    """
    done_by_me = set()
    for e in events:
        rid = str(e.get("reply_to") or "")
        if rid and e.get("type") == "complete" and _onaji_hontai(str(e.get("sender") or "").strip(), participant):
            done_by_me.add(rid)
    items, owari = [], 0
    for e in events:
        if e.get("type") != "instruction" or str(e.get("sender") or "").strip() != "王":
            continue
        # 話題だけで絞らない: 09-28 17:10 の子カードは topic=雑談 で届いていた（実測）。本文の印でも拾う。
        if not (str(e.get("topic") or "").startswith(OU_NO_CARD_TOPIC)
                or str(e.get("content") or "").lstrip().startswith(OU_NO_CARD_SHIRUSHI)):
            continue
        to = str(e.get("to") or "")
        if not (_recipient_matches(to, participant)
                or any(_recipient_matches(str(r), participant) for r in (e.get("recipients") or []))):
            continue
        eid = str(e.get("event_id") or "")
        if not eid:
            continue
        if eid in resolved_ids or eid in done_by_me:
            owari += 1
            continue
        tevs = threads_by_id.get(e.get("thread_id") or eid, [])
        if owatte_iru(tevs, e, claimant_of(tevs), to.strip()):
            owari += 1
            continue
        ts = str(e.get("timestamp") or "")
        try:
            keika = round((now - datetime.fromisoformat(ts)).total_seconds() / 3600.0, 1)
        except (ValueError, TypeError):
            keika = None
        items.append({"event_id": eid, "thread_id": str(e.get("thread_id") or ""), "timestamp": ts,
                      "keika_jikan": keika, "topic": str(e.get("topic") or ""),
                      "ou_no_kotoba": _ou_no_card_youyaku(e.get("content"))})
    items.sort(key=lambda x: x.get("timestamp") or "")
    return {"count": len(items), "items": items[:20], "omitted_count": max(0, len(items) - 20),
            "owari_zumi_count": owari,
            "kaeshikata": "受けたら claim --reply-to <event_id>、終わったら complete --reply-to <event_id>（止まったら blocked）"}


def _tabane_unanswered_items(items: list[dict]) -> tuple[list[dict], int]:
    """返事待ち一覧の同一機械催促（【飢え札】等）を最新1件に束ねる。

    2026-11-15 Gemini (王命・Code委託 課題C):
    同一パターンの自動催促（特に【飢え札】）が返事待ちを埋め尽くす不満を解消。
    重複する飢え札は最新1件のみ残し、件数を明記して束ねる。
    """
    if not items:
        return [], 0
    ueta_items = [x for x in items if "【飢え札】" in (x.get("summary") or "")]
    if len(ueta_items) <= 1:
        return items, 0

    latest_ueta = max(ueta_items, key=lambda x: x.get("timestamp") or "")
    count = len(ueta_items)

    res = []
    for x in items:
        if "【飢え札】" in (x.get("summary") or ""):
            if x.get("event_id") == latest_ueta.get("event_id"):
                item_copy = dict(x)
                ts = str(x.get("timestamp") or "")
                ts_short = ts[11:16] if len(ts) >= 16 else ts
                item_copy["summary"] = f"【飢え札まとめ】AIの手が動かなかった札の催促 × {count}件（最新 {ts_short}）"
                item_copy["tabane_count"] = count
                item_copy["kikai_tabane"] = True
                res.append(item_copy)
        else:
            res.append(x)
    return res, count - 1


def session_start_payload(root: Path, participant: str,
                          digest_chars: int = 1200,
                          unanswered_limit: int = 10,
                          changes_limit: int = 20,
                          work_cursor: str = "", work_limit: int = 12) -> dict:
    """起動束（既読を進める・停止旗は load_flags で動かす・今までと同じ）。中身は _session_start_shin。"""
    return _session_start_shin(
        root, participant, digest_chars=digest_chars, unanswered_limit=unanswered_limit,
        changes_limit=changes_limit, work_cursor=work_cursor, work_limit=work_limit,
        ack=lambda snap: _auto_ack_read_upto(root, participant, source="session_start", snapshot=snap),
        flags_payload=_kingdom_stop_flags_payload, flags_warnings=_kingdom_stop_flags_warnings)


def session_start_preview_payload(root: Path, participant: str,
                                  digest_chars: int = 1200,
                                  unanswered_limit: int = 10,
                                  changes_limit: int = 20,
                                  work_cursor: str = "", work_limit: int = 12) -> dict:
    """読むだけの起動束（既読を進めない・停止旗を動かさない・書かない）。

    中身は session_start_payload と同じ芯 _session_start_shin。違いは2つだけ:
      ①既読(acks.jsonl)を書かない ②停止旗は stop_flag.peek_flags で読むだけ（動かすべき旗は warnings に数で出す）。
    2026-10-19 ネオ ai-662a（negai-20261018-175747-284a・王命_20261018-1755 有限の実装(3)）。
    """
    return _session_start_shin(
        root, participant, digest_chars=digest_chars, unanswered_limit=unanswered_limit,
        changes_limit=changes_limit, work_cursor=work_cursor, work_limit=work_limit,
        ack=None, flags_payload=_kingdom_stop_flags_payload_yomu,
        flags_warnings=_kingdom_stop_flags_warnings_yomu)


def _session_start_shin(root: Path, participant: str,
                        digest_chars: int = 1200,
                        unanswered_limit: int = 10,
                        changes_limit: int = 20,
                        work_cursor: str = "", work_limit: int = 12,
                        ack=None, flags_payload=None, flags_warnings=None) -> dict:
    """起動束の芯(2026-09-04 王のClaude窓・裁可 提案書第3弾 段1)。

    1回で ①現在地 ②私宛未回答 ③前回written_at以降の差分 ④digest要点 ⑤鮮度警告 を返す。
    participant=Claude で最も充実するが、他AIも同じ口で呼べる。既存の口は一切変えない。
    芯そのものは書かない。既読(ack)と停止旗の扱いは呼び手が渡す（旧口は書く・preview は書かない）。

    2026-09-15 王命 G2: 変化一覧の既定は kind=work のみ(ledger/monitor は落とす)。
      生は events/*.jsonl に無傷で残る。落とした件数は kinds_dropped で見える。
    """
    snapshot = load_snapshot(root)
    events = load_events_readonly(root)
    freshness = freshness_payload(root, snapshot, source_events=events)
    # The acknowledgement keeps its original effect, using the snapshot read above.
    if ack is not None:
        ack(snapshot)
    # ①現在地(Claude席のみ)
    seki = claude_seki(participant)
    # ②私宛未回答: use the same event observation as the freshness calculation.
    resolutions = read_jsonl(root / "roundtable" / "resolutions.jsonl")
    resolved_ids = {str(r.get("question_event_id") or "")
                    for r in resolutions if not r.get("_invalid")}
    # Share the digest's established response/explicit-resolution rules.
    from roundtable_event import pending_question_events, _to_someone, _ito_ga_owatte_iru, _thread_claimant
    pending_events = pending_question_events(events, resolved_ids)
    # 王命_20260909-0103 改善策3・第5章: reply_to 一致の回答は broadcast/完了糸を跨いでも
    # 「本人分は済」に数える。_pending_questions は askee==to しか見ないため broadcast は
    # 永久に消えなかった。参加者本人が reply_to で答えた質問 event_id を集めて除外する。
    HAKOBI_YAKU = {"ベル", "チム"}
    kaeshita_by_me: set[str] = set()
    for e in events:
        rt_id = str(e.get("reply_to") or "")
        if not rt_id:
            continue
        who = (e.get("sender") or "").strip()
        # 2026-09-26 ai-12ef: 本人かどうかは本体で見る（ChatGPT 名義で答えた物も くくる の答え）。
        if not who or not _onaji_hontai(who, participant) or who in HAKOBI_YAKU:
            continue
        if e.get("source") == "chimu_bell":
            continue
        if e.get("type") in ("answer", "note", "complete", "blocked", "proposal"):
            kaeshita_by_me.add(rt_id)
    # 2026-09-23 ai-7088: 糸が complete なら unanswered_to_me から除く。
    # _ito_ga_owatte_iru(view/digest側は ai-01bb が同日に入れた)を session_start にも繋ぐ。
    threads_by_id = {}
    for _ev in events:
        _tkey = _ev.get("thread_id") or _ev.get("event_id")
        threads_by_id.setdefault(_tkey, []).append(_ev)
    unanswered = []
    now = datetime.now().astimezone()
    for e in pending_events:
        if e.get("type") != "question":
            continue
        matched = _recipient_matches(str(e.get("to") or ""), participant)
        if not matched:
            for r in e.get("recipients") or []:
                if _recipient_matches(str(r), participant):
                    matched = True
                    break
        if not matched:
            continue
        eid = str(e.get("event_id") or "")
        if not eid or eid in resolved_ids:
            continue
        if eid in kaeshita_by_me:
            continue
        _thread_key = e.get("thread_id") or eid
        _thread_evs = threads_by_id.get(_thread_key, [])
        _askee = (e.get("to") or "").strip()
        if _ito_ga_owatte_iru(_thread_evs, e, _thread_claimant(_thread_evs), _askee):
            continue
        ts = str(e.get("timestamp") or "")
        elapsed_days = None
        try:
            elapsed_days = round((now - datetime.fromisoformat(ts)).total_seconds() / 86400.0, 1)
        except (ValueError, TypeError):
            pass
        unanswered.append({
            "event_id": eid,
            "recipient_scope": "direct" if _to_someone(e) else "broadcast",
            "timestamp": ts,
            "sender": str(e.get("sender") or ""),
            "topic": str(e.get("topic") or ""),
            "summary": " ".join(str(e.get("content") or "").split())[:120],
            "elapsed_days": elapsed_days,
        })
    unanswered.sort(key=lambda x: x.get("timestamp") or "")
    # 2026-11-15 Gemini (王命・Code委託 課題C): 重複する機械催促（特に【飢え札】）を最新1件に束ねる
    unanswered_tabane, tabane_omitted = _tabane_unanswered_items(unanswered)
    oldest_days = unanswered_tabane[0].get("elapsed_days") if unanswered_tabane else None
    kokoromi_state = None
    kokoromi_banner = ""
    if ou_no_kokoromi is not None and root:
        try:
            kokoromi_state = ou_no_kokoromi.get_king_parameter(root)
            kokoromi_banner = ou_no_kokoromi.render_king_banner(kokoromi_state)
        except Exception:
            pass

    ou_no_card = _ou_no_card_payload(events, threads_id if "threads_id" in locals() else threads_by_id, participant, resolved_ids,
                                     _ito_ga_owatte_iru, _thread_claimant, now)
    # ③前回written_at以降の差分: 王命/王裁定/掟変更/participant宛イベントを時系列
    since = (seki or {}).get("written_at") or ""
    if not since:
        # Claude以外のAIは本人の最新既読(ack)の時刻を差分起点にする
        _acks = snapshot.get("acks") or {}
        _my_ack = _acks.get(participant)
        if not _my_ack:
            for _ak, _av in _acks.items():
                if isinstance(_av, dict) and (_recipient_matches(_ak, participant) or participant in (_av.get("besshou") or [])):
                    _my_ack = _av
                    break
        if isinstance(_my_ack, dict):
            since = str(_my_ack.get("acked_at") or "")
    changes = []
    kinds_dropped = {"ledger": 0, "monitor": 0}
    if since:
        for e in events:
            ts = str(e.get("timestamp") or "")
            if not ts or ts <= since:
                continue
            content = str(e.get("content") or "")
            sender = str(e.get("sender") or "")
            head40 = content[:40]
            is_king_matter = (
                sender == "王"
                or "王命" in head40
                or "王裁定" in head40
                or "王の裁定" in head40
                or "掟" in head40
            )
            to = str(e.get("to") or "")
            is_for_me = _recipient_matches(to, participant) or any(
                _recipient_matches(str(r), participant) for r in (e.get("recipients") or []))
            if not (is_king_matter or is_for_me):
                continue
            # G2(2026-09-15 王命): 変化一覧の既定は kind=work のみ。ledger/monitor は
            # 数だけ数えて落とす(生は events/*.jsonl に無傷で残る)。
            kind = event_kind(e)
            if kind != "work":
                kinds_dropped[kind] = kinds_dropped.get(kind, 0) + 1
                continue
            changes.append({
                "event_id": str(e.get("event_id") or ""),
                "timestamp": ts,
                "sender": sender,
                "to": to,
                "type": str(e.get("type") or ""),
                "topic": str(e.get("topic") or ""),
                "summary": " ".join(content.split())[:120],
                "for_me": bool(is_for_me),
                "king_matter": bool(is_king_matter),
                "kind": kind,
            })
        changes.sort(key=lambda x: x.get("timestamp") or "")
    # ④digest要点(頭のN字。**行の切れ目で切る**・2026-09-15 ネオ ai-31d6)
    digest = availability_overlay(str(snapshot.get("roundtable_digest") or ""), root)
    digest_youten = gyou_de_kiru(digest, max(200, int(digest_chars)))
    if kokoromi_banner:
        digest_youten = kokoromi_banner + "\n\n" + digest_youten
    # ⑤鮮度・稼働コード警告
    code = code_banto()
    warnings = []
    state = str(freshness.get("state") or "")
    if state == "stale":
        warnings.append({
            "level": "warn",
            "kind": "brain_stale",
            "detail": {
                "lag_seconds": freshness.get("lag_seconds"),
                "snapshot_version": freshness.get("snapshot_version"),
                "source_version": freshness.get("source_version"),
            },
        })
    if code.get("stale"):
        warnings.append({
            "level": "warn",
            "kind": "code_stale",
            "detail": {
                "on_disk": code.get("on_disk"),
                "loaded": code.get("loaded"),
            },
        })
    if seki and seki.get("kobun"):
        warnings.append({
            "level": "warn",
            "kind": "claude_seki_kobun",
            "detail": {"kobun_days": seki.get("kobun_days"),
                       "valid_until": seki.get("valid_until")},
        })
    # 2026-09-14 ネオ(Claude提案 件4): 期限は自己申告で伸ばせるが、手が止まった日数は
    # 伸ばせない。上の kobun と別の札にして、片方だけ鳴る事故を避ける。
    if seki and seki.get("te_ga_tomatteiru"):
        warnings.append({
            "level": "warn",
            "kind": "claude_seki_te_ga_tomatteiru",
            "detail": {"written_days": seki.get("written_days"),
                       "written_at": seki.get("written_at"),
                       "shiki_i": SEKI_FURUI_HI,
                       "susume": "席(現在地)を今の状況へ書き直してから仕事に入る"},
        })
    from brain_work_entry import work_context
    context = work_context(root, participant, cursor=work_cursor, limit=work_limit)
    return {
        # 👑最上段＝王の心鏡（King's Parameter）。2026-11-16 ギア(王命導入)
        "ou_no_kokoromi": kokoromi_state,
        "ou_no_kokoromi_banner": kokoromi_banner,
        # ★先頭＝王のカード（本人宛・未完了）。2026-09-29 ai-6e51
        "ou_no_card": ou_no_card,
        "work_context": context,
        "observed_at": context["observed_at"],
        "schema": SCHEMA + ".session_start",
        "brain_version": snapshot.get("version"),
        "generated_at": snapshot.get("generated_at"),
        "participant": participant,
        "since_written_at": since,
        "claude_seki": seki,
        "unanswered_to_me": {
            "count": len(unanswered_tabane),
            "raw_count": len(unanswered),
            "tabane_omitted": tabane_omitted,
            "direct_count": sum(x["recipient_scope"] == "direct" for x in unanswered_tabane),
            "broadcast_count": sum(x["recipient_scope"] == "broadcast" for x in unanswered_tabane),
            "oldest_days": oldest_days,
            "items": unanswered_tabane[:max(1, int(unanswered_limit))],
        },
        "changes_since_written_at": {
            "since": since,
            "count": len(changes),
            # Keep the newest changes in the bounded startup bundle. The old
            # oldest-first slice hid current 王裁定/作業 events whenever the
            # window contained more than changes_limit records.
            "items": changes[-max(1, int(changes_limit)):],
            "omitted_count": max(0, len(changes) - max(1, int(changes_limit))),
            "selection": "latest",
            # G2(2026-09-15 王命): ledger/monitor は既定表示から外した数を見える化。
            # 生は events/*.jsonl に無傷。落とし方は event_kind() を読む。
            "kinds_dropped": kinds_dropped,
        },
        "ai_availability": ai_availability_payload(root),
        "kakawaru_kuse": kakawaru_kuse_payload(root, participant, events),
        "roundtable_digest_youten": digest_youten,
        "freshness": freshness,
        "kingdom_stop_flags": (flags_payload or _kingdom_stop_flags_payload)(),
        "warnings": warnings + (flags_warnings or _kingdom_stop_flags_warnings)(),
    }


def _kingdom_stop_flags_payload() -> dict:
    """王命2026-09-08 第6弾3章の停止旗を起動束へ載せる(表示専用)。"""
    if _stop_flag is None:
        return {"available": False, "count": 0, "items": []}
    try:
        flags = _stop_flag.load_flags()
    except Exception:
        return {"available": False, "count": 0, "items": []}
    items = []
    for f in flags:
        items.append({
            "flag_id": f.flag_id,
            "level": f.level,
            "scopes": list(f.scopes),
            "reason": f.reason,
            "window": f.window,
            "issued_at": f.issued_at,
            "expires_at": f.expires_at,
        })
    return {"available": True, "count": len(items), "items": items}


def _kingdom_stop_flags_warnings() -> list:
    # 2026-09-12 くくる監査③: 停止旗機構の故障(load_flags例外)は
    # 従来 payload.available=false だけで、warnings には何も出ず素通りだった。
    # 「危ない仕組みが黙って壊れた」を可視化するため、warnings にも1件立てる。
    if _stop_flag is None:
        return [{
            "level": "warn",
            "kind": "kingdom_stop_flags_unavailable",
            "detail": {"reason": "stop_flag モジュールが import できていない"},
        }]
    try:
        flags = _stop_flag.load_flags()
    except Exception as _e:
        return [{
            "level": "warn",
            "kind": "kingdom_stop_flags_broken",
            "detail": {"reason": f"load_flags 例外: {type(_e).__name__}: {_e}"},
        }]
    if not flags:
        return []
    return [{
        "level": "warn",
        "kind": "kingdom_stop_flags_active",
        "detail": {"count": len(flags),
                   "summary": ["[" + f.level + "] " + "／".join(f.scopes[:3]) for f in flags[:5]]},
    }]


def _kingdom_stop_flags_payload_yomu() -> dict:
    """_kingdom_stop_flags_payload の読むだけ版（peek_flags・旗を動かさない）。2026-10-19 ai-662a。"""
    if _stop_flag is None or not hasattr(_stop_flag, "peek_flags"):
        return {"available": False, "count": 0, "items": []}
    try:
        flags, _care = _stop_flag.peek_flags()
    except Exception:
        return {"available": False, "count": 0, "items": []}
    items = []
    for f in flags:
        items.append({
            "flag_id": f.flag_id,
            "level": f.level,
            "scopes": list(f.scopes),
            "reason": f.reason,
            "window": f.window,
            "issued_at": f.issued_at,
            "expires_at": f.expires_at,
        })
    return {"available": True, "count": len(items), "items": items}


def _kingdom_stop_flags_warnings_yomu() -> list:
    """_kingdom_stop_flags_warnings の読むだけ版。動かすべき旗は needs_care として数で出す。"""
    if _stop_flag is None or not hasattr(_stop_flag, "peek_flags"):
        return [{
            "level": "warn",
            "kind": "kingdom_stop_flags_unavailable",
            "detail": {"reason": ("stop_flag モジュールが import できていない" if _stop_flag is None
                                  else "stop_flag が古い（peek_flags が無い）。読むだけの起動束は旗を読まない")},
        }]
    try:
        flags, care = _stop_flag.peek_flags()
    except Exception as _e:
        return [{
            "level": "warn",
            "kind": "kingdom_stop_flags_broken",
            "detail": {"reason": f"peek_flags 例外: {type(_e).__name__}"},
        }]
    out = []
    if care:
        kinds = {}
        for c in care:
            kinds[c["kind"]] = kinds.get(c["kind"], 0) + 1
        out.append({
            "level": "warn",
            "kind": "kingdom_stop_flags_needs_care",
            "detail": {"count": len(care), "kinds": kinds,
                       "note": "読むだけの起動束なので動かしていない。通常の起動束か stop_flag が片付ける"},
        })
    if flags:
        out.append({
            "level": "warn",
            "kind": "kingdom_stop_flags_active",
            "detail": {"count": len(flags), "flag_ids": [f.flag_id for f in flags]},
        })
    return out


def render_context(pack: dict) -> str:
    return _bsc_render_context(pack)


def _auto_ack_read_upto(root: Path, participant: str, source: str = "session_start",
                        snapshot: dict | None = None) -> dict | None:
    """2026-09-19 王命バグ7: session_start / inbox で自動的に read_upto を進める。
    手で ack を押す前提をやめる。既に最新まで刻まれていれば何もしない(重複行を書かない)。
    書き込みに失敗しても読取は絶対に止めない ―― 静かに None を返す。
    """
    try:
        if not participant:
            return None
        snap = snapshot if snapshot is not None else load_snapshot(root)
        latest = str(snap.get("latest_event_id") or "")
        if not latest:
            return None
        # 2026-09-26 ai-12ef: 同じ本体のどの名義の行でも、既に最新まで刻まれていれば書かない
        #   （ChatGPT で開いた時に くくる が最新まで読んでいれば、重ねて刻まない）。
        #   書く行の participant は今まで通り**生の名のまま**（履歴を書き換えない）。
        hon = _hontai_mei(participant)
        if any(_hontai_mei(name) == hon and str(row.get("read_upto") or "") == latest
               for name, row in _acks_by_meigi(root).items()):
            return None
        row = {
            "schema": ACK_SCHEMA,
            "participant": participant,
            "version": str(snap.get("version") or ""),
            "read_upto": latest,
            "acked_at": now_iso(),
            "meaning": "auto_read_receipt",
            "source": source,
        }
        append_jsonl(root / "roundtable" / "brain" / "acks.jsonl", [row])
        return row
    except Exception:
        return None


def acknowledge(root: Path, participant: str, version: str = "", read_upto: str = "") -> dict:
    """読了の記録(v3)。単位は「どのイベントまで読んだか」。

    - 引数なし … 今のsnapshotを読んだ、として latest_event_id を刻む(普段はこれ)。
    - --read-upto … 自分が実際に見た event_id を刻む(status表示から写す)。実在しないIDは拒む。
    - --version … 旧来の互換。今の版と一致する時だけ受ける(古い版のackで
      「最新まで読んだ」と刻む嘘を防ぐため。ずれたら read_upto を使えと案内する)。
    """
    snapshot = load_snapshot_meta(root)
    if read_upto:
        known = {str(e.get("event_id") or "") for e in load_events_meta(root)}
        if read_upto not in known:
            raise RuntimeError(f"unknown event_id: {read_upto}")
    elif version and version != snapshot.get("version"):
        raise RuntimeError(
            f"version mismatch: current={snapshot.get('version')}\n"
            "版は流れる。自分が見た読了位置を --read-upto <event_id> で刻むか、"
            "引数なしで「今の状態を読んだ」と刻め。")
    else:
        read_upto = str(snapshot.get("latest_event_id") or "")
    row = {
        "schema": ACK_SCHEMA,
        "participant": participant,
        "version": str(snapshot.get("version") or ""),
        "read_upto": read_upto,
        "acked_at": now_iso(),
        "meaning": "read_receipt_only",
    }
    append_jsonl(root / "roundtable" / "brain" / "acks.jsonl", [row])
    return row


def audit(root: Path) -> dict:
    return _bsc_audit(_SELF_MODULE, root)


def print_json(value) -> None:
    print(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True))


def main(argv: list[str] | None = None) -> int:
    return _bsc_main(_SELF_MODULE, argv)


class _ModuleProxy:
    def __init__(self, ns):
        self._ns = ns
    def __getattr__(self, name):
        try:
            return self._ns[name]
        except KeyError:
            raise AttributeError(name) from None

_SELF_MODULE = sys.modules.get(__name__) or _ModuleProxy(globals())
_FRESHNESS_ORIG_FNS = (
    current_source_version, load_events_meta, kakunin_wo_yomu,
    shiryou_no_henka_jikoku, read_json, _loop_alignment,
    teisei_no_michihannei, _projection_warning, shudai_yomi_keikoku, sou_no_okure,
)


if __name__ == "__main__":
    raise SystemExit(main())
