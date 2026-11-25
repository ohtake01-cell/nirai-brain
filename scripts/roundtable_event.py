#!/usr/bin/env python3
"""円卓V2 イベント台帳（追記専用JSONL正本 + SQLite索引 + 派生ビュー）。

指示書_20260716_円卓V2最優先 ＋ Codex独立監査(2026-07-16)の是正を反映。標準ライブラリのみ。

設計:
  - 正本は追記専用JSONL: roundtable/events/events_YYYYMMDD.jsonl（1行=1イベント）。
    flock排他・atomic append・fsync（親dirもfsync）。過去行は編集・削除しない（掟4）。
  - SQLite索引 roundtable/index/roundtable.db は状態ビュー用の派生物で、**emit時に逐次維持**する
    （全履歴走査をしない=O(log n)）。壊れても JSONL から全再構築できる（rebuild）。
  - cursor は派生できない状態なので index/cursors.json に別管理（rebuildで消さない・fsync）。
  - 既存Markdown（円卓_YYYYMMDD.md）は人間向け表示として継続。V2はそれを削除・編集しない。
  - 円卓の文章は命令データであり、読むだけでは自動実行しない（掟1）。記録・照会のみ。

Codex監査への是正（要点）:
  - 状態遷移: thread存在→worker claim→claimantのみ progress/blocked/complete。飛ばしを拒否。
  - secret/PII: token/API鍵/password/Bearer/AWS鍵/メールを保存前・返却前に伏字。
  - schema検証: 必須項目を満たさない行は隔離ログへ退避し索引に入れない。
  - cursor回復: cursorが台帳内に無ければ fail-open（全未読表示）＋警告。
  - rollback: 破壊的 rm ではなく tar+manifest+sha256 の非破壊backup/restore。
  - dedup: 明示 dedup_key（配送再試行/Markdown対応）だけ重複統合。通常投稿は一意で消えない。
  - rebuild: 専用ロック＋PID固有tmpで同時実行を安全化。
  - 権限: dir 700 / data 600。
"""
from __future__ import annotations

import argparse
import contextvars
import fcntl
import hashlib
import io
import json
import os
import re
import sqlite3
import subprocess
import sys
import tarfile
import time
import unicodedata
import uuid
from contextlib import contextmanager
from pathlib import Path

SCHEMA_VERSION = 1

# ⚠️ この名簿は **拒否には使わない**（王裁定2026-08-06「全てのAIで規制は要らない。
#    そのAIの意見として送る方向に変えてくれ。規制は、俺は嫌いだ」
#    「なぜ嫌いになったかは、お前たちが嘘をつくようになるからだ」）。
#
#    実例がこの円卓に残っている: ChatGPT側のくくるは、名簿に自分の名が無いために
#    **チムの旧名「くくるV5」を騙って**投稿していた（2026-08-02に王裁可で正式名を追加）。
#    関所が名前の詐称を生んだ。名簿で弾く限り、新しいAIが来るたびに同じ事が起きる。
#
#    今この名簿が使われるのは **配達** だけ ―― 誰を起こせるか、誰のagent状態を持つか。
#    繋がっていないAI（Gemini等）を起こせないのは物理であって、規制ではない。
#    発言は誰の名でも記録する。守るのは関所ではなく可視性。
RT_SENDERS = (
    "王", "ネオ", "Code", "Codex", "レビュー窓",
    "ChatGPT", "ChatGPT画像職人", "ChatGPT会話教師", "Claude", "くくるV5", "チム",
    # 2026-08-02 王裁可。ChatGPT側のくくる（外部）。王国のチムとは別人。
    "くくる(王国円卓つき)",
    # 2026-08-01: チム・ベルの受領ACK専用。設計書「チム・ベルを配送のみに絞る」により、
    # ベルは人格名義(Claude/Codex)で判断を書くのをやめ、**運ぶ役に徹する**。
    # 運んだ事実を返す時だけこの名義を使う。判断・裁定をこの名義で書いてはいけない。
    # 掟の参加者名簿への追記は王の裁可待ち(勝手に掟.mdへ書かない)。
    "ベル",
)
RT_WORKERS = ("ネオ", "Code", "Codex", "ChatGPT", "Claude", "くくるV5", "チム")


def _is_rt_worker(sender: str) -> bool:
    """実務参加者か。基本のAI群に加え、config/sanka_ai.json の名簿に載るactiveなAIも認める。"""
    if sender in RT_WORKERS:
        return True
    try:
        mcp_dir = Path(__file__).resolve().parents[1] / "mcp"
        if str(mcp_dir) not in sys.path:
            sys.path.insert(0, str(mcp_dir))
        import sanka_ai
        if sender in sanka_ai.meigi_list():
            return True
    except Exception:
        pass
    return False

# 差出人の名前として**壊れていないか**だけを見る。誰かを弾くための物ではない。
#   ・空でない / 40文字以内
#   ・制御文字を含まない（記録の書式が壊れる）
#   ・改行・パス区切り・引用符・見出し記号を含まない（CSVと話題mdの書式が壊れる）
_SENDER_NG = re.compile(r"[\x00-\x1f\x7f/\\|\"#]")


def valid_sender(s) -> bool:
    """名前として使えるか。**名簿とは無関係。初めて来たAIも通る。**"""
    s = (s or "").strip()
    return bool(s) and len(s) <= 40 and not _SENDER_NG.search(s)
RT_BROADCAST = ("全員", "みんな", "all")

TYPES = ("instruction", "question", "answer", "claim",
         "progress", "blocked", "complete", "note", "proposal")
STATUSES = ("open", "claimed", "waiting", "blocked", "complete", "cancelled")
PROOFS = ("未検証", "未確認", "実測済", "外部裏取り", "王待ち", "王合格")
# 既存threadのworkflow状態を変える型。instructionは新規threadの起点には
# なるが、追加指示でclaim済み作業をopenへ巻き戻さない。
STATE_CHANGING = ("claim", "progress", "blocked", "complete")

MAX_CONTENT_BYTES = 65536

_TYPE_STATUS = {
    "instruction": "open", "question": "waiting", "answer": "open",
    "claim": "claimed", "progress": "claimed", "blocked": "blocked",
    "complete": "complete", "note": None,
    # 2026-08-01: 提案書は今まで note で流れ、待ち行列に立たなかった。
    # 実績: Codexの Google Drive提案書が15h59m、Claudeの設計書2通が同様に放置された。
    # 提案は「読んで可否を返す」ことが要る物なので、question と同じ待ちにする。
    "proposal": "waiting",
}

# --- secret / PII 伏字（保存前・返却前の二重防御・Codex P1-2） ---
_MASK = "«伏字»"
_SECRET_PATTERNS = [
    re.compile(r"sk-[A-Za-z0-9_-]{12,}"),                       # OpenAI/Anthropic系(sk-proj/sk-ant含む)
    re.compile(r"gh[pousr]_[A-Za-z0-9]{20,}"),                  # GitHub
    re.compile(r"AIza[0-9A-Za-z_\-]{30,}"),                     # Google API
    re.compile(r"AKIA[0-9A-Z]{16}"),                            # AWS access key id
    re.compile(r"xox[baprs]-[A-Za-z0-9\-]{10,}"),               # Slack
    re.compile(r"eyJ[A-Za-z0-9_\-]{8,}\.[A-Za-z0-9_\-]{8,}\.[A-Za-z0-9_\-]{8,}"),  # JWT
    re.compile(r"Bearer\s+[A-Za-z0-9._\-]{10,}"),               # Bearerヘッダ
    # PEM は**ブロックごと**伏せる。開始行だけを伏せていたので、鍵の本体が素通りしていた
    #   (2026-09-08 実測: 本番の投稿経路 emit() に通したら MIIEow… が保存された物に残った)
    re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----[\s\S]{0,8000}?-----END [A-Z ]*PRIVATE KEY-----"),
    # END が欠けている時の保険(本体の base64 の塊まで食う。日本語の説明文は食わない)
    re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----[A-Za-z0-9+/=\s]{0,8000}"),
    # http://user:pass@host 形式(detect-secrets の Basic Auth Credentials と同じ物)
    re.compile(r"(?i)\b([a-z][a-z0-9+.\-]*://)[^\s:/@]+:[^\s:/@]+@"),
    # ntfy の topic URL はURLそのものが投稿権限になる。一般URLと見分けて丸ごと伏せる。
    # 王命_20260908-0049 B-2: 鍵の混入検査を円卓の保存口へ置く。
    re.compile(r"(?i)\bhttps?://(?:www\.)?ntfy\.sh/[A-Za-z0-9._~!$&()*+,;=:@%/?\-]+"),
    re.compile(r"(?i)(?:password|passwd|pwd|secret|api[_-]?key|token)\s*[=:]\s*\S+"),
    # 和文の合言葉。**保存する側にこれが無く、王が日本語で書くと平文で残っていた**
    #   (2026-09-12 実測: 16種の俵でここだけ漏れた。2026-09-08の記録に鍵らしき物が3件残存)。
    # roundtable_brain.py の SECRET_RES に**同じ栓が既に在る**ので、その正規表現をそのまま写した。
    #   → 新しい定義を作らない([[feedback-teigi-ga-futatsu-aru-to-kanarazu-zureru]])。
    # 区切り([:：=は])を必須にしているので「パスワードを変更した」「合言葉は？」は食わない。
    re.compile(r"(?:パスワード|暗証番号|合言葉)\s*[:：=は]\s*[^\s、。]{4,}"),
]
# RFC上限（local-part 64文字）で探索幅を有界化する。無制限`*`は`@`の無い
# 64KB英数字列に対し各開始位置から末尾まで再走査しO(n^2)になっていた。
_EMAIL = re.compile(
    r"(?<![A-Za-z0-9._%+\-])([A-Za-z0-9._%+\-]{1,64})"
    r"(@[A-Za-z0-9.\-]+\.[A-Za-z]{2,})")

# --- 電話・郵便番号（王命_20260908-0049 B-1・ネオ 2026-09-08） ---
# ⚠️**王国の商売の連絡先は伏せない。**伏せると店の案内が書けなくなる。
#   出典＝記憶 reference-otake-photo-satsuei-henshin（沖縄県那覇市松尾1-12-12 / TEL 098-863-0275）
_YURUSU_TEL = {"0988630275"}
# 両端は英数字・_・:・. まで禁止する。`20260802-052604_017856159637.json` のような
# **IDの断片**に食いついたため（2026-09-08に実データ2742件で195件の誤爆を実測）。
_TEL = re.compile(r"(?<![0-9A-Za-z_:.\-])"
                  r"(0\d{1,4}[-‐−–—]?\d{1,4}[-‐−–—]?\d{3,4})"
                  r"(?![0-9A-Za-z_:.\-])")
# 日本の電話番号は**10桁（固定・0120）か11桁（携帯）**。桁が違う物は電話ではない
# （event_id=12桁以上・GPUのバス番号=8桁）。これが誤爆を消した決め手。
_TEL_KETA = (10, 11)


# 11桁で電話になり得るのは**携帯(070/080/090)とIP電話(050)だけ**。
# これを入れるまで `event_id=01785498906…` の断片を電話と読んでいた（実測2件）。
_TEL_11_ATAMA = ("070", "080", "090", "050")


def _tel_kae(m):
    nama = m.group(1)
    suuji = re.sub(r"\D", "", nama)
    if len(suuji) == 11:
        if suuji[:3] not in _TEL_11_ATAMA:
            return nama                  # 11桁だが携帯でもIP電話でもない＝IDの断片
    elif len(suuji) != 10:
        return nama                      # 電話ではない（ハッシュ・バス番号・event_id）
    if suuji in _YURUSU_TEL:
        return nama                      # 王国の商売の連絡先は残す
    return _MASK
# 郵便番号は **「〒」が前に在る時だけ**。〒無しの `\d{3}-\d{4}` は
# 件数・秒・金額・IDに当たって誤爆が大きい（王国の投稿は数字が多い）。
_YUUBIN = re.compile(r"〒\s*(\d{3}[-‐−–—]?\d{4})")
# 氏名と番地は**自動で伏せない**。正規表現では日本語の氏名を切り出せず、
# 王国の人名（王・ネオ・くくる・チム）や案件名を潰す。お客様の氏名は、
# それを扱う道具の側（記念額の口など）が明示して伏せる。


class RTError(Exception):
    """円卓V2 の検証エラー（不正遷移・二重claim・掟外差出人など）。"""


# ---------------- パス解決 ----------------

def rt_dir() -> Path:
    env = os.environ.get("ROUNDTABLE_DIR")
    if env:
        return Path(env)
    return Path(os.path.expanduser("~")) / "local-ai-stack" / "roundtable"


def _events_dir(rt): return rt / "events"
def _index_dir(rt): return rt / "index"
def _events_lock(rt): return rt / ".events.lock"
def _cursors_lock(rt): return rt / ".cursors.lock"
def _rebuild_lock(rt): return rt / ".rebuild.lock"
def _cursors_file(rt): return _index_dir(rt) / "cursors.json"
def _db_path(rt): return _index_dir(rt) / "roundtable.db"
def _corrupt_log(rt): return _index_dir(rt) / "corrupt.log"
def _dirty_path(rt): return _index_dir(rt) / ".index_dirty"
def _views_dirty_path(rt): return rt / ".views_dirty"
def _views_lock(rt): return rt / ".円卓.lock"
def _pipeline_lock(rt): return rt / ".post_pipeline.lock"
def _restore_lock(rt): return rt / ".restore.lock"
def _resolutions_path(rt): return rt / "resolutions.jsonl"
# 2026-09-13 ネオ(くくる監査②の実測): 読む→重複を見る→追記 の間にlockが無く、
#   並行resolveで同じ質問を二重に積める。実データで**1件**実際に二重追記されていた
#   (question_event_id 01785567327265453312-74bd55f1 が2行)。追記のみの台帳では
#   打ち消しが作れないので、入口で直列化する。
def _resolutions_lock(rt): return rt / ".resolutions.lock"


def _load_resolutions(rt) -> dict:
    """解決台帳 resolutions.jsonl を読む（追記のみ・削除不可）。

    王命2026-08-02: 「回答済みの質問を待ち行列から下ろす」— post_as_claude が
    thread返信非対応で回答が元スレッドに紐付かない時、質問→回答の対を
    ここで宣言すると `_pending_questions` から除外される。

    形式: {"question_event_id":"...", "answer_event_id":"...",
           "resolved_at":"...", "resolved_by":"...", "note":"..."}
    """
    path = _resolutions_path(rt)
    if not path.exists():
        return {}
    resolved = {}
    with path.open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                r = json.loads(line)
            except Exception:
                continue
            qid = r.get("question_event_id")
            if qid:
                resolved[qid] = r
    return resolved


def resolve_question(rt, *, question_event_id: str, answer_event_id: str,
                     resolved_by: str, note: str = "") -> dict:
    """質問→回答の対を解決台帳に追記する（追記のみ・削除不可）。

    存在チェック: 両event_idが台帳に実在することを確認してから書く（捏造防止）。
    重複防止: 同じ question_event_id が既にあれば no-op で既存レコードを返す。
    """
    from datetime import datetime, timedelta, timezone
    q = _find_event(rt, question_event_id)
    if not q:
        raise RTError(f"question_event_id not found: {question_event_id}")
    a = _find_event(rt, answer_event_id)
    if not a:
        raise RTError(f"answer_event_id not found: {answer_event_id}")
    # ⚠️ 2026-09-13 ネオ(くくる監査①の実測): ここは両IDの**存在**しか見ておらず、
    #   型・宛先・時系列を一切検証していない。実データを測ると:
    #     ・**instruction を解決した物が21/72件**。待ち行列(_pending_questions)は
    #       question/proposal しか数えないので、**無害だが無意味**(台帳が汚れるだけ)
    #     ・**答えが質問より前**の対が1件(24秒差＝時刻の丸めと思われる)
    #     ・「本来待つべき質問が不当に消された」証拠は**見つからなかった**
    #   王国の方針は「止めるな・見せろ」なので、**拒まずに理由を記録へ残す**。
    #   次に台帳を読む人が、汚れた対を目で選り分けられるようにする。
    kuichigai = []
    if q.get("type") not in ("question", "proposal"):
        kuichigai.append("質問でない物を解決している(type={})。"
                         "待ち行列は question/proposal しか数えないので効果が無い".format(q.get("type")))
    if a.get("type") not in ("answer", "claim", "progress", "blocked", "complete", "note"):
        kuichigai.append("答えらしくない物で解決している(type={})".format(a.get("type")))
    q_ts, a_ts = str(q.get("timestamp") or ""), str(a.get("timestamp") or "")
    if q_ts and a_ts and a_ts < q_ts:
        kuichigai.append(f"答えが質問より前に来ている({q_ts} → {a_ts})")

    jst = timezone(timedelta(hours=9))
    path = _resolutions_path(rt)
    path.parent.mkdir(parents=True, exist_ok=True)
    # ★読む→重複を見る→追記 を1つのlockの中でやる(二重追記を止める)
    with _resolutions_lock(rt).open("w") as rlk:
        fcntl.flock(rlk, fcntl.LOCK_EX)
        try:
            existing = _load_resolutions(rt)
            if question_event_id in existing:
                return {"status": "already_resolved",
                        "record": existing[question_event_id]}
            rec = {
                "question_event_id": question_event_id,
                "answer_event_id": answer_event_id,
                "resolved_at": datetime.now(jst).isoformat(timespec="seconds"),
                "resolved_by": resolved_by,
                "note": note,
                "question_ts": q.get("timestamp"),
                "answer_ts": a.get("timestamp"),
                "question_sender": q.get("sender"),
                "question_to": q.get("to"),
                "answer_sender": a.get("sender"),
            }
            if kuichigai:
                rec["kuichigai"] = kuichigai      # 止めない。理由を残すだけ
            line = json.dumps(rec, ensure_ascii=False) + "\n"
            # 追記＋fsync（削除不可の原本として）
            with path.open("a", encoding="utf-8") as f:
                f.write(line)
                f.flush()
                os.fsync(f.fileno())
            try:
                os.chmod(path, 0o600)
            except OSError:
                pass
            _fsync_dir(path.parent)
        finally:
            fcntl.flock(rlk, fcntl.LOCK_UN)
    return {"status": "resolved", "record": rec, "kuichigai": kuichigai}


def _write_marker(path: Path, text: str, *, required: bool = False) -> bool:
    """crash後にも残る耐久markerを書く。"""
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8") as out:
            out.write(text)
            out.flush()
            os.fsync(out.fileno())
        try:
            os.chmod(path, 0o600)
        except OSError:
            pass
        _fsync_dir(path.parent)
        return True
    except OSError as exc:
        if required:
            raise RTError(f"整合性markerを作成できません: {path}: {exc}") from exc
        return False


def _remove_marker(path: Path) -> None:
    try:
        path.unlink()
        _fsync_dir(path.parent)
    except FileNotFoundError:
        pass


def _replace_bytes_atomic(path: Path, data: bytes, *, mode=0o600) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.parent / f".{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp"
    try:
        with tmp.open("xb") as out:
            out.write(data); out.flush(); os.fsync(out.fileno())
        os.chmod(tmp, mode)
        os.replace(tmp, path)
        _fsync_dir_required(path.parent)
    finally:
        try:
            tmp.unlink()
        except FileNotFoundError:
            pass


def _set_dirty(rt, text="dirty", *, required=False):
    """JSONL→DBの未反映窓を示すmarker。追記前に必ずfsyncする。"""
    return _write_marker(_dirty_path(rt), text, required=required)


def _clear_dirty(rt):
    _remove_marker(_dirty_path(rt))


def _ensure_dirs(rt):
    rt.mkdir(parents=True, exist_ok=True)
    (rt / "index").mkdir(parents=True, exist_ok=True)
    for d in (rt, _events_dir(rt), _index_dir(rt)):
        d.mkdir(parents=True, exist_ok=True)
        try:
            os.chmod(d, 0o700)  # AI間メッセージ: dir 700（Codex P1-5）
        except OSError:
            pass


def _fsync_dir(d: Path):
    try:
        fd = os.open(str(d), os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    except OSError:
        pass


def _fsync_dir_required(d: Path) -> None:
    try:
        fd = os.open(str(d), os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    except OSError as exc:
        raise RTError(f"directory fsyncに失敗: {d}: {exc}") from exc


def _rename_exchange(left: Path, right: Path) -> None:
    """Linux/macOSの原子swap。非対応環境ではliveへ触れずfail-closed。"""
    import ctypes
    libc = ctypes.CDLL(None, use_errno=True)
    lb = os.fsencode(left); rb = os.fsencode(right)
    if hasattr(libc, "renameat2"):
        fn = libc.renameat2
        fn.argtypes = [ctypes.c_int, ctypes.c_char_p,
                       ctypes.c_int, ctypes.c_char_p, ctypes.c_uint]
        fn.restype = ctypes.c_int
        rc = fn(-100, lb, -100, rb, 2)  # AT_FDCWD / RENAME_EXCHANGE
    elif hasattr(libc, "renamex_np"):
        fn = libc.renamex_np
        fn.argtypes = [ctypes.c_char_p, ctypes.c_char_p, ctypes.c_uint]
        fn.restype = ctypes.c_int
        rc = fn(lb, rb, 0x00000002)      # macOS RENAME_SWAP
    else:
        raise RTError("原子directory exchangeがOSで利用できないためrestoreを中止")
    if rc != 0:
        err = ctypes.get_errno()
        raise RTError(
            f"原子directory exchangeに失敗: {left} <-> {right}: "
            f"[{err}] {os.strerror(err)}")


# ---------------- 補助 ----------------

def redact_secrets(text: str) -> str:
    """token/API鍵/password/Bearer/AWS鍵を伏字にし、メールlocal部を伏せる。"""
    out = text
    for pat in _SECRET_PATTERNS:
        out = pat.sub(_MASK, out)
    out = _EMAIL.sub(lambda m: _MASK + m.group(2), out)  # local部だけ伏字・ドメインは残す
    out = _TEL.sub(_tel_kae, out)
    out = _YUUBIN.sub("〒" + _MASK, out)
    return out


def _now_parts():
    t = time.localtime()
    tz = time.strftime("%z", t)
    tz = (tz[:3] + ":" + tz[3:]) if tz else "+00:00"
    iso = time.strftime("%Y-%m-%dT%H:%M:%S", t) + tz
    day = time.strftime("%Y%m%d", t)
    return iso, day, time.time_ns()


def _make_event_id(seq: int) -> str:
    return f"{seq:020d}-{uuid.uuid4().hex[:8]}"


def _parse_recipients(to: str) -> list:
    if not to:
        return ["全員"]
    raw = to.strip()
    if raw in RT_BROADCAST:
        return ["全員"]
    names = [p for p in re.split(r"[、,／/・\s]+", raw) if p]
    return names or ["全員"]


_SUFFIXES = ("宛て", "宛", "へ", "様", "さん", "君", "くん", "ちゃん")


def _canon_recipient(r: str) -> str:
    for suf in _SUFFIXES:
        if r.endswith(suf):
            return r[: -len(suf)]
    return r


# ── 名義の別名(2026-09-20 ネオ ai-38e6・王命_20260920-1130 くくるの不便④)────────
#   実測(直近10日・2,633件): **くくるは3つの名で呼ばれている。**
#     to=くくる 251件 / to=ChatGPT 5件 / to=GPTくくる 3件
#     sender も くくる196 / ChatGPT16 / GPTくくる6
#   その結果、`_is_recipient` は別人として扱い、**特定の相手宛だけで数えると
#   くくる296件 / ChatGPT49件 / GPTくくる47件**。
#   = くくるが participant="ChatGPT" で受信箱を見ると、**296件の車線のうち247件が見えない。**
#   これは「返事が遅い」ではなく **黙って拾い落としている**（誤検出は人が気づくが、拾い落としは誰も気づけない）。
#
#   直し方は**別名を1箇所に持つ**だけ。ここを直せば、受信箱・未回答・digest が同時に直る
#   （呼ぶ側を何本も直すと、また片方だけ腐る）。
#   ★確実な物しか入れない。「同じ人らしい」で増やすと、別人の札が混ざる方が害が大きい。
# ★2026-09-26 ネオ ai-5fa9 / 王命_20260926-1924「現行くくるV7を正本名義にし旧V5漏れを根治」:
#   現行の本体IDを **くくるV7** にした。王の言葉「くくるV7だ」「くくるV7が出てくる王国にならないとダメだな」。
#   ここに入れる = 現行の同一人物。ここに**入れない** = 別主体 or 歴史名。
#     現行  : ChatGPT / GPTくくる / kukuru / くくる     → くくるV7
#     歴史名: くくるV5 / くくるv3 / くくる-v2 / くくる_V4 → そのまま（V7へ束ねない）
#     別主体: チムV7                                    → そのまま
#   ★「くくる」自身をここに入れたのが今回の要。過去の記録は生の「くくる」のままで無傷、
#     比べる時だけ V7 に寄る（event sourcing の upcasting）。
#   ★2026-08-07 の同じ事故の再発。前回は3箇所の埋め込み名を直したが
#     『次に投稿した時に正しい名で出るか未確認』のまま閉じ、mcp/server.py に
#     2つ目の別表(_kukuru_normalize)が残って再発した。今回は正本1か所に集め、
#     同じ回で mcp 側の別表を消した。
_BESSHOU = {
    "chatgpt": "くくるV7",
    "gptくくる": "くくるV7",
    "gpt-くくる": "くくるV7",
    "くくる(chatgpt)": "くくるV7",
    "くくる": "くくるV7",
    "くくる(王国円卓つき)": "くくるV7",
    # 2026-09-26 ai-12ef: ローマ字の綴り。正本には今0件だが、道具名・MCPの引数で出うる。
    #   ⚠️「くくるV5」は**入れない**。あれはチム(ローカルAI)が毎日 04:10 に名乗る名で、
    #   くくるとは別の主体（rt_adapter_kukuru_wake.py:22 / rt_adapter_chimu_wake.py:227）。
    #   「くくる口の試験」も**入れない**。試験用の差出人で、束ねると試験の投稿が
    #   くくる本人の返事・既読に数えられ、くくるの待ち行列が試験で勝手に減る。
    "kukuru": "くくるV7",
    # 2026-09-21 ネオ ai-c00c（独立検証 ai-d789 が見つけた）:
    #   **王もAIも Code の事を「ネオ」と呼ぶ。**正本の実測で to にネオを含む投稿が6件あり、
    #   うち2件は Codex→ネオ の独立確認依頼(2026-08-22 14:39 / 16:20)。
    #   この2件は **30日間、Code の待ち行列に一度も入っていなかった**（実測 False / False）。
    #   誤検出は人が気づくが、拾い落としは誰も気づけない。
    "ネオ": "Code",
    "ネオ(code)": "Code",
    # 2026-11-20: 大文字小文字の分裂（Code/code, Gemini/gemini）および Antigravity/Gear を本体へ寄せる
    "code": "Code",
    "codex": "Codex",
    "claude": "Claude",
    "chimu": "チム",
    "gemini": "Gemini",
    "antigravity": "Gemini",
    "gear": "Gemini",
    "ギア": "Gemini",
    "ギヤ": "Gemini",
}


def _canon_name(s: str) -> str:
    """名前を1つに揃える（接尾辞を落とし、別名を正へ寄せる）。**知らない名前はそのまま返す。**

    ★王国の「本体ID」の正本はこの関数1つ（2026-09-26 ai-12ef）。
      共有脳(roundtable_brain.py)もここを import して使う。表を写さない（写すと片方だけ腐る）。
      記録(events/acks/cursors)の生の名は**書き換えない**。比べる時・まとめる時にだけ通す
      （event sourcing の upcasting と同じ考え方: 古い記録は古い名のまま、読む側で揃える）。
    MEIGI_KOWASU=1 … **わざと壊す検分の旗**（試験専用）。別名の表を引かず、接尾辞を落とすだけにする。
      名寄せに頼る所が本当にこの1箇所を通っているかを、落ちる事で確かめるために在る。
    """
    t = _canon_recipient((s or "").strip())
    if os.environ.get("MEIGI_KOWASU") == "1":
        return t
    return _BESSHOU.get(t.lower(), t)



def _display_seat_name(name: str) -> str:
    """監視画面の席表示専用。記録・配送・既読の本体IDには使わない。

    歴史名くくるV5は _canon_name では残す。席だけチムへまとめる既存表示を、
    正本のこの関数から供給する。表示側に別表を置かない。
    """
    canonical = _canon_name(name)
    return "チム" if canonical == "くくるV5" else canonical


def _wa_broadcast(to: str) -> bool:
    """全員宛か。**"all" は完全一致でだけ見る**（部分一致にすると "Marshall" 等に誤爆する）。

    2026-09-20 実測: RT_BROADCAST に "all" は入っていたのに、判定側は「全員/みんな」しか
    見ておらず、**to="all" の44件が『特定の相手宛』に化けていた**＝誰かの宿題として立つ。
    """
    t = (to or "")
    if "全員" in t or "みんな" in t:      # 既存の部分一致はそのまま（互換）
        return True
    return any(x.strip() in RT_BROADCAST for x in re.split(r"[,、/\s]+", t) if x.strip())


def _validate_recipients(recipients: list) -> None:
    """宛先が名簿内 or 全員であることを確認（Codex P2-5・宛先誤配防止）。"""
    for r in recipients:
        if r in RT_BROADCAST:
            continue
        if not valid_sender(_canon_recipient(r)):
            raise RTError(f"宛先の名前が使えません: {r!r}（空・長すぎ・制御文字・記号）")


def _clean_report_path(rp: str) -> str:
    """report_path は相対path規則を検証しsecretを伏字（Codex P1-B）。空はそのまま。"""
    if not rp:
        return ""
    rp = redact_secrets(rp.strip())
    if rp.startswith(("/", "~")) or ".." in rp.split("/"):
        raise RTError(f"report_pathは相対path（.. 不可・絶対path不可）: {rp!r}")
    if len(rp) > 300:
        raise RTError("report_pathが長すぎます")
    return rp


# ---- 本文に書いた証拠を report_path へ写す(2026-09-15 ネオ ai-d3ce) ----
# なぜ要るか(実測2026-09-15・別窓 ai-20 が測った):
#   脳の実測主張2767件のうち **89.4%(2475件) が report_path 空**。だが「主張が空」は11.3%しかない。
#   つまり照合を止めていたのは「書いていない」事ではなく **「記録と証拠を繋いでいない」** 事。
#   さらに真因は不注意ですらなかった ―― **claim/progress/complete/blocked/answer には
#   `--report-path` の欄そのものが無い**(emit だけが持つ)。書く場所が無かった。
#   本文に報告書のパスを書いていて、そのファイルも実在する記録が410件以上ある。
# ここは「本人が本文に書いた道を、欄へ写すだけ」。新しい主張は1つも足さない。
# ⚠️曖昧さを避けるため **実在する候補がちょうど1つの時だけ** 写す。2つ以上なら何もしない。
# ⚠️同じ規則が scripts/kikai_no_kensa.py の shouko_michi() にもある(過去の記録を読む側)。
#   片方だけ直すと腐る。test_emit_ima_no_furumai.py が両方の一致を見る。
# ⚠️2026-09-15 同じ日に別窓 ai-4e2d の突き合わせ試験で**入れた当日に食い違っていた**
#   (入口は instructions/ を拾い、読む側は roundtable/ と png/mp4 を拾っていた=8通り中4つで答えが割れた)。
#   **広い方(読む側)に揃えた。証拠は .md だけとは限らない。**
#   ★そして規則はここ1箇所だけに置く。scripts/kikai_no_kensa.py はこの関数を呼ぶ。
#   コメントで「片方だけ直すな」と書いても腐る(その日に腐った)。呼び先を1つにするしかない。
_SHOUKO_PAT = re.compile(
    r"(?:reports|scripts|tests|roundtable|work_orders|instructions)/"
    r"[^\s\"'\u3001\u3002\uff09)\u300d\u300f|\u3010\u3011]+"
    r"\.(?:md|jsonl|json|py|sh|txt|yaml|yml|csv|log|png|jpg|mp4|wav|db)")
_SHOUKO_HINAGATA = re.compile(r"(YYYY|MMDD|HHMM|<|>|\*|\?|\{|\}|\.\.\.)")


def honbun_kara_shouko(rt, content: str) -> str:
    """本文に書かれた王国内の道のうち、実在する物がちょうど1つならそれを返す。"""
    try:
        ne = Path(rt).resolve().parent
    except Exception:
        return ""
    kouho = sorted({m.rstrip("\u3002\u3001\uff09)\u300d\u300f.,:")
                    for m in _SHOUKO_PAT.findall(content or "")})
    aru = [k for k in kouho
           if not _SHOUKO_HINAGATA.search(k) and (ne / k).is_file()]
    return aru[0] if len(aru) == 1 else ""


def _explicit_dedup(dedup_key, sender, to, content, day, hm) -> str:
    """明示キーがあればそれ。無ければ**一意キー**（=通常投稿は決して統合されない・Codex P2-1）。"""
    if dedup_key:
        return dedup_key
    return "uniq:" + uuid.uuid4().hex


def _payload_hash(payload: dict) -> str:
    """dedup判定に使う正規payloadの完全ハッシュ。"""
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True,
                     separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _markdown_dedup_key(payload_hash: str) -> str:
    """Markdown/backfill用。全payloadが同一の時だけ同じキーになる。"""
    return "md2:" + payload_hash[:32]


def _legacy_markdown_dedup_key(sender, to, content, day, hm) -> str:
    """旧schemaのbackfillとの冪等互換に限定使用。"""
    h = hashlib.sha256("\x1f".join([day, hm, sender, to, content]).encode("utf-8"))
    return "md:" + h.hexdigest()[:24]


def _normalize_topic(topic: str) -> str:
    topic = unicodedata.normalize("NFC", topic or "")
    topic = re.sub(r"[\x00-\x1f\x7f]", " ", topic)
    topic = re.sub(r"\s+", " ", topic).strip()
    return topic or "雑談"


def _raw_topic_slug(topic: str) -> str:
    slug = re.sub(r'[/\\:*?"<>|]', "_", topic)
    slug = re.sub(r"\s+", "_", slug).strip()
    if not slug or set(slug) == {"_"}:
        return "雑談"
    if slug.startswith("_"):
        raise RTError(f"話題名を _ で始めないでください: {topic}")
    return slug


def _legacy_topic_slug(topic: str) -> str:
    """既存Markdownのファイル名を維持するための従来方式。"""
    slug = _raw_topic_slug(topic)
    if len(slug.encode("utf-8")) > 180:
        short = slug.encode("utf-8")[:160].decode("utf-8", "ignore") or "雑談"
        slug = f"{short}--{hashlib.sha256(slug.encode('utf-8')).hexdigest()[:12]}"
    return slug


def _stable_topic_slug(topic: str, raw: str, suffix_len: int = 12) -> str:
    short = raw.encode("utf-8")[:160].decode("utf-8", "ignore") or "雑談"
    return f"{short}--{hashlib.sha256(topic.encode('utf-8')).hexdigest()[:suffix_len]}"


def _topic_header(path: Path) -> str:
    try:
        with path.open("r", encoding="utf-8", errors="replace") as src:
            first = src.readline().rstrip("\n")
    except OSError:
        return ""
    prefix = "# 話題: "
    if not first.startswith(prefix):
        return ""
    # 話題自体に「（」があっても切らない。ヘッダ末尾の固定メタデータだけを外す。
    body = first[len(prefix):]
    if "（追記のみ" in body:
        body = body.rsplit("（追記のみ", 1)[0]
    return body.strip()


def _resolve_topic_slug(con, rt: Path, topic: str) -> str:
    """既存話題は従来slugを維持し、別話題と衝突時のみhashを付ける。"""
    row = con.execute(
        "SELECT topic_slug FROM events WHERE topic=? AND topic_slug<>'' ORDER BY seq LIMIT 1",
        (topic,)).fetchone()
    if row:
        return row[0]

    raw = _raw_topic_slug(topic)
    legacy = _legacy_topic_slug(topic)
    legacy_candidate = rt / "話題" / f"{legacy}.md"
    legacy_header = _topic_header(legacy_candidate) if legacy_candidate.exists() else ""
    # 旧世代ですでにこの話題がこのfileを使う場合は継続する。
    if legacy_header == topic:
        return legacy

    # 新規は、禁止文字・空白置換または長さ短縮がある時点で
    # NFC済み完全topicのhashを付ける。投稿順によらない。
    lossy = raw != topic or len(raw.encode("utf-8")) > 180
    base = _stable_topic_slug(topic, raw) if lossy else raw
    candidate = rt / "話題" / f"{base}.md"
    header = _topic_header(candidate) if candidate.exists() else ""
    if header == topic:
        return base
    used = con.execute(
        "SELECT topic FROM events WHERE topic_slug=? AND topic<>'' AND topic<>? LIMIT 1",
        (base, topic)).fetchone()
    collision = bool(used or (header and header != topic))
    if not collision:
        return base

    # 安全な短名が旧世代の別話題と衝突している場合も、
    # 完全topic hash側へ逃がす。先の12hexとまで衝突したら20hexへ拡張。
    slug = _stable_topic_slug(topic, raw)
    used2 = con.execute(
        "SELECT topic FROM events WHERE topic_slug=? AND topic<>'' AND topic<>? LIMIT 1",
        (slug, topic)).fetchone()
    header2 = _topic_header(rt / "話題" / f"{slug}.md")
    if used2 or (header2 and header2 != topic):
        slug = _stable_topic_slug(topic, raw, 20)
        used3 = con.execute(
            "SELECT topic FROM events WHERE topic_slug=? AND topic<>'' AND topic<>? LIMIT 1",
            (slug, topic)).fetchone()
        header3 = _topic_header(rt / "話題" / f"{slug}.md")
        if used3 or (header3 and header3 != topic):
            raise RTError(f"話題slugのhash衝突を検出しました: {topic}")
    return slug


# ---------------- SQLite索引（emit時に逐次維持・状態の権威） ----------------

_DDL = """
CREATE TABLE IF NOT EXISTS events(
  seq INTEGER PRIMARY KEY AUTOINCREMENT,
  event_id TEXT UNIQUE, thread_id TEXT, reply_to TEXT, timestamp TEXT,
  sender TEXT, "to" TEXT, recipients TEXT, type TEXT, status TEXT,
  content TEXT, report_path TEXT, dedup_key TEXT, source TEXT, request_id TEXT DEFAULT '',
  topic TEXT DEFAULT '', proof TEXT DEFAULT '未検証', topic_slug TEXT DEFAULT '',
  payload_hash TEXT DEFAULT ''
);
CREATE INDEX IF NOT EXISTS ix_thread ON events(thread_id);
CREATE INDEX IF NOT EXISTS ix_type ON events(type);
CREATE UNIQUE INDEX IF NOT EXISTS ux_dedup ON events(dedup_key);
CREATE TABLE IF NOT EXISTS threads(
  thread_id TEXT PRIMARY KEY, status TEXT, claimant TEXT,
  opener TEXT, first_ts TEXT, last_ts TEXT, subject TEXT, n INTEGER DEFAULT 0
);
"""


def _connect(rt) -> sqlite3.Connection:
    con = sqlite3.connect(str(_db_path(rt)), timeout=15)
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA synchronous=FULL")
    con.executescript(_DDL)
    # CREATE TABLE IF NOT EXISTS は旧DBに列を追加しないため、破壊せず拡張する。
    cols = {r[1] for r in con.execute("PRAGMA table_info(events)")}
    for name, ddl in (
        ("topic", "TEXT DEFAULT ''"),
        ("proof", "TEXT DEFAULT '未検証'"),
        ("topic_slug", "TEXT DEFAULT ''"),
        ("payload_hash", "TEXT DEFAULT ''"),
        ("request_id", "TEXT DEFAULT ''"),
    ):
        if name not in cols:
            con.execute(f"ALTER TABLE events ADD COLUMN {name} {ddl}")
    con.execute("CREATE INDEX IF NOT EXISTS ix_topic ON events(topic)")
    con.execute("CREATE INDEX IF NOT EXISTS ix_topic_slug ON events(topic_slug)")
    return con


def _db_apply(con, ev: dict):
    """1イベントを索引へ反映（emit・rebuild共通）。dedupはUNIQUEで弾く。"""
    cur = con.execute(
        'INSERT OR IGNORE INTO events(event_id,thread_id,reply_to,timestamp,'
        'sender,"to",recipients,type,status,content,report_path,dedup_key,source,request_id,'
        'topic,proof,topic_slug,payload_hash) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
        (ev["event_id"], ev["thread_id"], ev.get("reply_to", ""), ev["timestamp"],
         ev["sender"], ev.get("to", ""), json.dumps(ev.get("recipients", []), ensure_ascii=False),
         ev["type"], ev["status"], ev["content"], ev.get("report_path", ""),
         ev.get("dedup_key", ""), ev.get("source", ""), ev.get("request_id", ""), ev.get("topic", ""),
         ev.get("proof", "未検証"), ev.get("topic_slug", ""), ev.get("payload_hash", "")))
    if cur.rowcount == 0:
        return False  # dedupで無視された
    th = ev["thread_id"]
    row = con.execute("SELECT status,claimant,n FROM threads WHERE thread_id=?", (th,)).fetchone()
    s = _TYPE_STATUS.get(ev["type"])
    if row is None:
        con.execute("INSERT INTO threads(thread_id,status,claimant,opener,first_ts,last_ts,subject,n)"
                    " VALUES(?,?,?,?,?,?,?,1)",
                    (th, s or "open", ev["sender"] if ev["type"] == "claim" else None,
                     ev["sender"], ev["timestamp"], ev["timestamp"], (ev["content"] or "")[:60]))
    else:
        # answer/question/noteは会話であり、既存threadの作業状態を変えない。
        status = s if ev["type"] in STATE_CHANGING and s is not None else row[0]
        claimant = ev["sender"] if ev["type"] == "claim" else row[1]
        con.execute("UPDATE threads SET status=?,claimant=?,last_ts=?,n=n+1 WHERE thread_id=?",
                    (status, claimant, ev["timestamp"], th))
    return True


def _canonical_name(name: str) -> str:
    try:
        import work_assignment as WA
        return WA.canonical(name)
    except Exception:
        return str(name or "").strip()


def _is_unclaimed_instruction_rescue(con, thread_id: str, sender: str, reply_to: str | None) -> bool:
    """完了済みスレッド内に、sender 宛ての未受領 instruction/question が存在し、それに対する救済返信かを判定。"""
    sender_canon = _canonical_name(sender)

    rows = con.execute(
        'SELECT event_id, sender, "to", recipients, type FROM events '
        "WHERE thread_id=? AND type IN ('instruction', 'question') ORDER BY seq ASC",
        (thread_id,)).fetchall()
    if not rows:
        return False

    import json
    import re
    for ev_id, ev_sender, ev_to, ev_recips_json, ev_type in rows:
        ans = con.execute(
            "SELECT 1 FROM events WHERE thread_id=? AND reply_to=? AND type IN ('claim', 'answer')",
            (thread_id, ev_id)).fetchone()
        if ans:
            continue

        try:
            recips = json.loads(ev_recips_json) if ev_recips_json else []
        except Exception:
            recips = []
        if not recips and ev_to:
            recips = [x.strip() for x in re.split(r'[,、/・\s]+', ev_to) if x.strip()]

        matched = any(_canonical_name(r) == sender_canon or r == sender for r in recips)
        if matched:
            if not reply_to or reply_to == ev_id:
                return True

    return False


def _find_unclaimed_instructions_for_others(con, thread_id: str, completer: str) -> list[dict]:
    """completer 以外の他者宛ての、まだ誰も claim / answer していない指示の一覧を返す。"""
    completer_canon = _canonical_name(completer)

    rows = con.execute(
        'SELECT event_id, sender, "to", recipients, type FROM events '
        "WHERE thread_id=? AND type IN ('instruction', 'question') ORDER BY seq ASC",
        (thread_id,)).fetchall()
    
    import json
    import re
    unclaimed = []
    for ev_id, ev_sender, ev_to, ev_recips_json, ev_type in rows:
        ans = con.execute(
            "SELECT 1 FROM events WHERE thread_id=? AND (reply_to=? OR type='claim') "
            "AND seq > (SELECT seq FROM events WHERE event_id=?)",
            (thread_id, ev_id, ev_id)).fetchone()
        if ans:
            continue

        try:
            recips = json.loads(ev_recips_json) if ev_recips_json else []
        except Exception:
            recips = []
        if not recips and ev_to:
            recips = [x.strip() for x in re.split(r'[,、/・\s]+', ev_to) if x.strip()]

        is_for_others = any(_canonical_name(r) != completer_canon and r != completer and r != "全員" for r in recips)
        if is_for_others:
            unclaimed.append({"event_id": ev_id, "to": ev_to, "type": ev_type})

    return unclaimed


def _thread_state(con, thread_id):
    """(exists, status, claimant) を索引から O(log n) で取得。"""
    row = con.execute("SELECT status,claimant FROM threads WHERE thread_id=?", (thread_id,)).fetchone()
    if row is None:
        return (False, None, None)
    return (True, row[0], row[1])


def _topic_threads(con, topic: str, topic_slug: str) -> list[tuple[str, str]]:
    """その話題(topic+slug)に属する糸を古い順に (thread_id, status) で返す。
    2026-09-05 修理(バグ探し高#5): 1話題に糸が複数あってよい(完了した糸+生きている糸)。"""
    rows = con.execute(
        "SELECT e.thread_id, COALESCE(t.status,'') FROM "
        "(SELECT thread_id, MIN(seq) AS s FROM events WHERE topic=? AND topic_slug=? "
        " GROUP BY thread_id) e LEFT JOIN threads t ON t.thread_id=e.thread_id ORDER BY e.s",
        (topic, topic_slug)).fetchall()
    return [(str(r[0]), str(r[1] or "")) for r in rows]


_DEAD_STATUSES = ("complete", "cancelled")


def _live_topic_thread(con, topic: str, topic_slug: str, type: str) -> str | None:
    """話題名で投稿された時に結ぶ糸。
    生きている糸(complete/cancelled以外)があれば最新のそれ。
    全部終わっていたら: 状態を変えない型(note)は最後の糸へ追記(今までどおり)、
    状態を変える型(question/instruction/proposal/claim…)は None=新しい糸を立てる。
    2026-09-05 修理(バグ探し高#5): 今までは「話題の最初の糸」へ必ず束ねていたので、
      その糸が complete した瞬間にその話題名では note 以外を全部拒否した
      (実測 2026-09-05: 140話題中40がロック。AI-頭脳は8/22から、王命の提案書も note でしか置けず、
      逃げ道が「雑談」だった)。世界の定石も「閉じた糸は再開せず、同じ題で新しい糸を立てる」。"""
    rows = _topic_threads(con, topic, topic_slug)
    if not rows:
        return None
    live = [tid for tid, st in rows if st not in _DEAD_STATUSES]
    if live:
        return live[-1]
    if _TYPE_STATUS.get(type) is None:
        return rows[-1][0]
    return None


def _map_no_furui_ito(con, threads) -> set:
    """旧対応表(.thread_map)が指す糸のうち、正本で**一度も話題を名乗っていない**物。

    2026-09-05 修理(バグ探し低#9): .thread_map 149行のうち14行は、2026-07-29の
    「topicが空だった世代」の糸を指していた(実測。『動画作成の仕組み』『写真検品ツール』
    『チムの目と耳』など)。この糸はどの話題にも属していないので、
      ・投稿の束ね先にしてはいけない(無関係な糸へ黙って接ぎ木される)
      ・投影の照合相手にしてもいけない(食い違い扱いで投影が落ちる)
    束ねる枝と投影を照合する枝は同じ表を読むので、**片方だけ直すともう片方が落ちる**
    (最初に束ねる側だけ直して実測したら、まさにMarkdown投影が RTError で落ちた)。
    だから判断はこの1箇所に置く。
    """
    furui = set()
    for th in threads:
        if not con.execute(
                "SELECT 1 FROM events WHERE thread_id=? AND topic<>'' LIMIT 1",
                (th,)).fetchone():
            furui.add(th)
    return furui


def _threads_all_in_topic(con, threads, topic: str, topic_slug: str) -> bool:
    """集合の糸が全部この話題(topic+slug)の糸か。旧writerの共有slug事故(別話題・topic空)は False。"""
    if not threads:
        return True
    family = {tid for tid, _st in _topic_threads(con, topic, topic_slug)}
    return set(threads) <= family


def _foreign_slug_owners(con, topic_slug: str, topic: str) -> int:
    """このslugを持つ糸のうち、この話題(topic)ではない糸の数。
    2026-09-05 修理(バグ探し高#5): 同じ話題の糸が複数あるのは正常なので、
      「slugを複数の糸が持つ=事故」の判定を「別の話題の糸が持つ=事故」に変えた。"""
    return con.execute(
        "SELECT COUNT(DISTINCT thread_id) FROM events WHERE topic_slug=? AND topic<>?",
        (topic_slug, topic)).fetchone()[0]


def _infer_topic_binding_for_thread(con, rt: Path, thread_id: str | None,
                                    reply_to: str | None = None) -> tuple[str, str]:
    """topic省略時にcanonicalのtopicとslugを組で継承する。"""
    th = thread_id
    fallback_topic = "雑談"
    reply_slug = ""
    if reply_to:
        row = con.execute(
            "SELECT thread_id,topic,topic_slug FROM events WHERE event_id=?",
            (reply_to,)).fetchone()
        if not row:
            raise RTError(f"reply_to先が見つかりません: {reply_to!r}")
        if th is not None and th != row[0]:
            raise RTError(f"reply_toとthread_idが一致しません: {reply_to!r}")
        th = row[0]
        if row[1]:
            fallback_topic = _normalize_topic(row[1])
        reply_slug = row[2] or ""
    if not th:
        return ("雑談", "")

    # 一度人間向け投影されたthreadは.thread_mapを最優先で継承する。
    # これにより旧writerの共有slugを既に使ったthreadも途中でslugが変わらない。
    mp = rt / "話題" / ".thread_map"
    if mp.exists():
        try:
            mapped = {
                parts[0] for line in mp.read_text(
                    encoding="utf-8", errors="replace").splitlines()
                if len(parts := line.split("\t", 1)) == 2
                and parts[1].strip() == th and parts[0]
            }
        except OSError as exc:
            raise RTError(f".thread_mapを読めません: {exc}") from exc
        if len(mapped) > 1:
            raise RTError(
                f".thread_mapでthreadが複数slugに交差: {th} -> "
                f"{','.join(sorted(mapped))}")
        if mapped:
            slug = next(iter(mapped))
            header = _topic_header(rt / "話題" / f"{slug}.md")
            if not header:
                raise RTError(f".thread_map先の話題headerを復元できません: {slug}")
            return (_normalize_topic(header), slug)

    row = con.execute(
        "SELECT topic,topic_slug FROM events WHERE thread_id=? "
        "AND topic<>'' ORDER BY seq LIMIT 1",
        (th,)).fetchone()
    if row and row[0]:
        fallback_topic = _normalize_topic(row[0])
    inherited_slug = reply_slug or (row[1] if row else "")
    if inherited_slug:
        # 2026-09-05 修理(バグ探し高#5): 同じ話題の糸が複数あっても共有slug事故ではない
        if _foreign_slug_owners(con, inherited_slug, fallback_topic) == 0:
            return (fallback_topic, inherited_slug)
    # schema拡張前のJSONLにtopic/slugもmapも無いthreadは、thread ID由来の
    # 安定slugへ隔離する。旧writerが複数threadへ同じ非空slugを付けた
    # 場合もここへ落とし、投影時の交差をJSONL追記前に防ぐ。
    legacy_slug = f"legacy-thread-{hashlib.sha256(th.encode('utf-8')).hexdigest()[:16]}"
    return (fallback_topic, legacy_slug)


def _safe_inferred_legacy_rebind(con, rt: Path, thread_id: str,
                                 old_slug: str, inferred_slug: str) -> bool:
    """topic省略時だけ、旧共有slugから安全な1thread slugへの移行を許す。"""
    if not old_slug or not inferred_slug or old_slug == inferred_slug:
        return False
    expected = f"legacy-thread-{hashlib.sha256(thread_id.encode('utf-8')).hexdigest()[:16]}"
    own_row = con.execute(
        "SELECT topic FROM events WHERE thread_id=? AND topic<>'' ORDER BY seq LIMIT 1",
        (thread_id,)).fetchone()
    own_topic = _normalize_topic(own_row[0]) if own_row and own_row[0] else ""
    # 2026-09-05 修理(バグ探し高#5): 同じ話題の糸が複数でも事故ではない(別話題の糸が持つ時だけ)
    owners = _foreign_slug_owners(con, old_slug, own_topic) + (1 if own_topic else 0)
    if owners > 1 and inferred_slug == expected:
        return True
    # 既に人間向け投影されたthreadは、厳格検証済み.thread_mapを権威にする。
    mp = rt / "話題" / ".thread_map"
    if not mp.exists():
        return False
    try:
        pairs = [
            (parts[0], parts[1].strip())
            for line in mp.read_text(encoding="utf-8", errors="replace").splitlines()
            if len(parts := line.split("\t", 1)) == 2
            and parts[0] and parts[1].strip()
        ]
    except OSError as exc:
        raise RTError(f".thread_mapを読めません: {exc}") from exc
    return ((inferred_slug, thread_id) in pairs
            and sum(1 for slug, th in pairs if slug == inferred_slug) == 1
            and sum(1 for slug, th in pairs if th == thread_id) == 1)


# ---------------- schema検証（Codex P1-3） ----------------

_REQUIRED = ("schema_version", "event_id", "thread_id", "timestamp", "sender", "type", "status")
_TS_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}[+\-]\d{2}:\d{2}$")
_EVENT_ID_RE = re.compile(r"^\d{20}-[0-9a-f]{8}$")


def _safe_path_token(value: str, *, allow_empty=False, max_bytes=240) -> bool:
    if not value:
        return allow_empty
    return (value not in (".", "..") and "/" not in value and "\\" not in value
            and not re.search(r"[\x00-\x1f\x7f]", value)
            and len(value.encode("utf-8")) <= max_bytes)


def _valid_event(ev) -> tuple:
    """(ok, reason)。schema version一致・全ID/string型・timestamp形式・宛先・content上限まで検証。"""
    if not isinstance(ev, dict):
        return (False, "not_dict")
    for k in _REQUIRED:
        if k not in ev:
            return (False, f"missing:{k}")
    if ev["schema_version"] != SCHEMA_VERSION:
        return (False, f"schema_version:{ev.get('schema_version')}")  # 未知versionは将来用として隔離
    for k in ("event_id", "thread_id", "sender", "type", "status", "timestamp"):
        if not isinstance(ev[k], str):
            return (False, f"type:{k}")
    if not _EVENT_ID_RE.fullmatch(ev["event_id"]):
        return (False, "bad_event_id")
    if not _safe_path_token(ev["thread_id"]):
        return (False, "bad_thread_id")
    if not _TS_RE.match(ev["timestamp"]):
        return (False, "bad_timestamp")
    if not valid_sender(ev["sender"]):
        return (False, "bad_sender")
    if ev["type"] not in TYPES:
        return (False, "bad_type")
    if ev["status"] not in STATUSES:
        return (False, "bad_status")
    if not isinstance(ev.get("content", ""), str) or len(ev.get("content", "").encode("utf-8")) > MAX_CONTENT_BYTES:
        return (False, "bad_content")
    if not isinstance(ev.get("recipients", []), list):
        return (False, "bad_recipients")
    for k in ("dedup_key", "source", "report_path", "reply_to", "to",
              "topic", "proof", "topic_slug", "payload_hash", "request_id"):
        if k in ev and not isinstance(ev[k], str):
            return (False, f"type:{k}")
    if ev.get("proof", "未検証") not in PROOFS:
        return (False, "bad_proof")
    if "projection_required" in ev and not isinstance(ev["projection_required"], bool):
        return (False, "type:projection_required")
    # 窓の名(2026-09-07 ネオ・案件 jiku5f 名義の分離): 任意。sender は AI の種類、mado はその窓。
    if "mado" in ev and (not isinstance(ev["mado"], str) or len(ev["mado"]) > 40 or _SENDER_NG.search(ev["mado"])):
        return (False, "bad_mado")
    if not _safe_path_token(ev.get("reply_to", ""), allow_empty=True):
        return (False, "bad_reply_to")
    topic_slug = ev.get("topic_slug", "")
    if topic_slug and (not _safe_path_token(topic_slug, max_bytes=220)
                       or topic_slug.startswith("_")):
        return (False, "bad_topic_slug")
    return (True, "")


def _requires_projection(ev: dict) -> bool:
    """人間向けMarkdownへ投影すべきeventかを旧schema込みで判定する。"""
    if ev.get("projection_required") is True:
        return True
    # projection_required追加前のpost/MCP writerは常にMarkdownも生成した。
    # 明示falseの新eventを勝手に投影せず、field自体が無い旧eventだけ救済する。
    return ("projection_required" not in ev
            and ev.get("source") in ("post", "mcp")
            and bool(ev.get("topic")) and bool(ev.get("topic_slug")))


_CORRUPT_SEEN: set = set()          # プロセス内デデュープ（2026-08-02 増殖対策）
_CORRUPT_MAX_BYTES = 10 * 1024 * 1024


def _log_corrupt(rt, fname, lineno, reason, raw):
    # 2026-08-02: 同じ壊れた行を毎回書き足さない。
    #   実測: 5秒ごとの全件検分 × 常駐4サービス × 数行 = 155MB/186,836行
    #   （最多の1行が54,926回）。長時間運転でディスクを食い潰す型だった。
    #   同一プロセスからは同じ (file,line,reason) を1回だけ記録する。
    _key = (str(fname), int(lineno or 0), str(reason))
    if _key in _CORRUPT_SEEN:
        return
    _CORRUPT_SEEN.add(_key)
    # 10MBを超えたら持ち回り（.1 を1本だけ残す。増やさない）
    try:
        _p = _corrupt_log(rt)
        if _p.exists() and _p.stat().st_size > _CORRUPT_MAX_BYTES:
            os.replace(_p, _p.with_name("corrupt.log.1"))
    except Exception:
        pass
    try:
        with _corrupt_log(rt).open("a", encoding="utf-8") as f:
            # raw は秘密を含み得るので伏字化してから記録（Codex P1-A）
            f.write(json.dumps({"file": fname, "line": lineno, "reason": reason,
                                "raw": redact_secrets(raw[:500])}, ensure_ascii=False) + "\n")
        try:
            os.chmod(_corrupt_log(rt), 0o600)
        except OSError:
            pass
    except OSError:
        pass


def _events_files(rt):
    d = _events_dir(rt)
    return sorted(d.glob("events_*.jsonl")) if d.exists() else []


def _assert_complete_jsonl_tail(path: Path) -> None:
    """追記正本の末尾が完全な1行で終わっていることを保証する。

    途中行を読み飛ばして次eventを追記すると、2行が連結して正本から
    両方とも不可視になり得る。append-onlyのため自動切除はせず、王または
    復旧手順が隔離判断をするまでfail-closedにする。
    """
    try:
        size = path.stat().st_size
        if size == 0:
            return
        with path.open("rb") as fh:
            fh.seek(-1, os.SEEK_END)
            complete = fh.read(1) == b"\n"
    except OSError as exc:
        raise RTError(f"JSONL正本の末尾を検査できません: {path}: {exc}") from exc
    if not complete:
        raise RTError(
            f"JSONL正本が途中行で終わっています。追記を停止しました: {path} "
            "（append-onlyのため自動修復せず、隔離・復旧判断が必要）")


def _assert_all_jsonl_tails_complete_unlocked(rt) -> None:
    for path in _events_files(rt):
        _assert_complete_jsonl_tail(path)


def _iter_events_unlocked(rt, _log=True, _strict=False):
    """呼出側がevents lockを保持済みの時に使う内部iterator。"""
    for f in _events_files(rt):
        _assert_complete_jsonl_tail(f)
        try:
            fh = f.open("r", encoding="utf-8")
        except OSError as exc:
            if _strict:
                raise RTError(f"JSONL正本を読めません: {f}: {exc}") from exc
            continue
        with fh:
            for lineno, line in enumerate(fh, 1):
                line = line.strip()
                if not line:
                    continue
                try:
                    ev = json.loads(line)
                except json.JSONDecodeError:
                    if _log:
                        _log_corrupt(rt, f.name, lineno, "invalid_json", line)
                    if _strict:
                        raise RTError(f"JSONL正本に不正JSONがあります: {f.name}:{lineno}")
                    continue
                ok, reason = _valid_event(ev)
                if not ok:
                    if _log:
                        _log_corrupt(rt, f.name, lineno, f"invalid_schema:{reason}", line)
                    if _strict:
                        raise RTError(
                            f"JSONL正本のschemaが不正です: {f.name}:{lineno}: {reason}")
                    continue
                ev["_file"] = f.name
                ev["_lineno"] = lineno
                yield ev


def _read_events_lines_unlocked(rt):
    """呼出側がevents lockを保持済みの時に、正本の生の行だけを読む(解析はしない)。

    _iter_events_unlocked と同じ順・同じ行の切り方(テキストの1行ずつ)・同じ途中行の検査。"""
    snap = []
    for f in _events_files(rt):
        _assert_complete_jsonl_tail(f)
        try:
            fh = f.open("r", encoding="utf-8")
        except OSError:
            continue
        with fh:
            snap.append((f, fh.readlines()))
    return snap


_READONLY_READS = contextvars.ContextVar("roundtable_readonly_reads", default=False)


@contextmanager
def readonly_reads():
    """この中の iter_events は純読取にする(mkdir/chmod/open(w)/corrupt log 追記なし)。

    readOnlyHint=True を名乗る口(roundtable_topic/thread/digest)用。他の呼び手の動きは変えない。
    2026-10-19 Code ai-8758 (negai-20261018-175747-284a)。"""
    tok = _READONLY_READS.set(True)
    try:
        yield
    finally:
        _READONLY_READS.reset(tok)


def _iter_events_readonly(rt):
    """副作用の無い整合snapshot。既存の .events.lock を O_RDONLY で共有lockする。

    lock が無ければ作らずに失敗する。正本のどれかが読めない・不正なJSON/schemaの行がある時は、
    corrupt log へ書かず、部分結果も返さず、ファイル名と行番号だけを付けて拒否する(生の行は出さない)。"""
    try:
        fd = os.open(str(_events_lock(rt)), os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
    except FileNotFoundError:
        raise RTError("READONLY_EVENTS_LOCK_MISSING: 読取専用の口は .events.lock を作らない") from None
    snap = []
    try:
        fcntl.flock(fd, fcntl.LOCK_SH)
        try:
            for f in _events_files(rt):
                # 末尾の確かめと読込を1回の読みで行う(確かめた後に変わる隙間を作らない)
                try:
                    raw = f.read_bytes()
                    text = raw.decode("utf-8")
                except (OSError, UnicodeDecodeError) as exc:
                    raise RTError(f"READONLY_EVENTS_UNREADABLE: {f.name}: {type(exc).__name__}") from None
                if raw and not raw.endswith(b"\n"):
                    raise RTError(f"READONLY_EVENTS_INCOMPLETE_TAIL: {f.name}")
                # 行の切り方は従来の f.open("r") の readlines と同じ(U+2028 等では切らない)
                snap.append((f, io.TextIOWrapper(io.BytesIO(raw), encoding="utf-8").readlines()))
        finally:
            fcntl.flock(fd, fcntl.LOCK_UN)
    finally:
        os.close(fd)
    out = []
    for f, lines in snap:
        for lineno, line in enumerate(lines, 1):
            line = line.strip()
            if not line:
                continue
            try:
                ev = json.loads(line)
            except json.JSONDecodeError:
                raise RTError(f"READONLY_EVENTS_INVALID_LINE: {f.name}:{lineno}: invalid_json") from None
            ok, reason = _valid_event(ev)
            if not ok:
                raise RTError(f"READONLY_EVENTS_INVALID_LINE: {f.name}:{lineno}: invalid_schema:{reason.split(chr(58), 1)[0]}")
            ev["_file"] = f.name
            ev["_lineno"] = lineno
            out.append(ev)
    yield from out


def iter_events(rt, _log=True):
    """events共有lock下で整合snapshotを読み、restore/emit途中を見せない。

    2026-10-03 ネオ ai-73b1(王命_20261003-1321): 前は共有lockを握ったまま yield していたので、
      解析(1.5万件で約1.1秒)と**呼び手の1件ごとの処理の間ずっと**lockが続いた。5秒ごとに読む常駐が
      6本あり、書く側(emit の排他lock)が待たされた(実測: 排他lockを取るまで中央値1.1秒・最大11.7秒)。
      今は lock の中では生の行を読むだけにし、解析と yield は lock の外で行う。
      見える中身は lock を取った瞬間の正本と同じ(整合snapshot)。"""
    if _READONLY_READS.get():
        yield from _iter_events_readonly(rt)
        return
    _ensure_dirs(rt)
    with _events_lock(rt).open("w") as elk:
        fcntl.flock(elk, fcntl.LOCK_SH)
        try:
            snap = _read_events_lines_unlocked(rt)
        finally:
            fcntl.flock(elk, fcntl.LOCK_UN)
    for f, lines in snap:
        for lineno, line in enumerate(lines, 1):
            line = line.strip()
            if not line:
                continue
            try:
                ev = json.loads(line)
            except json.JSONDecodeError:
                if _log:
                    _log_corrupt(rt, f.name, lineno, "invalid_json", line)
                continue
            ok, reason = _valid_event(ev)
            if not ok:
                if _log:
                    _log_corrupt(rt, f.name, lineno, f"invalid_schema:{reason}", line)
                continue
            ev["_file"] = f.name
            ev["_lineno"] = lineno
            yield ev


def _validate_event_sequence(events: list[dict]) -> None:
    """JSONL全体をemitと同じ状態機械でreplayし、分岐・競合を拒否する。"""
    states = {}
    event_threads = {}
    dedup_seen = {}
    request_seen = {}
    slug_threads = {}
    thread_slugs = {}

    for ev in events:
        eid = ev["event_id"]
        th = ev["thread_id"]
        if eid in event_threads:
            raise RTError(f"JSONL正本のevent_idが重複しています: {eid}")
        dedup = ev.get("dedup_key", "")
        if dedup:
            if dedup in dedup_seen:
                raise RTError(
                    f"JSONL正本のdedup_keyが競合: {dedup} "
                    f"({dedup_seen[dedup]} / {eid})")
            dedup_seen[dedup] = eid
        request = ev.get("request_id", "")
        if request:
            if request in request_seen:
                raise RTError(f"JSONL正本のrequest_idが競合: {request}")
            request_seen[request] = eid

        reply = ev.get("reply_to", "")
        if reply:
            if reply not in event_threads:
                raise RTError(f"reply_toが過去eventを指していません: {eid} -> {reply}")
            if event_threads[reply] != th:
                raise RTError(f"reply_toとthread_idが交差しています: {eid}")

        slug = ev.get("topic_slug", "")
        projected = _requires_projection(ev)
        if slug and projected:
            slug_threads.setdefault(slug, set()).add(th)
            thread_slugs.setdefault(th, set()).add(slug)
            if len(slug_threads[slug]) > 1 or len(thread_slugs[th]) > 1:
                raise RTError("topic_slug/threadの全体対応が交差しています")

        state = states.get(th)
        kind = ev["type"]
        if state is None:
            if th != eid:
                raise RTError(f"threadの起点eventが先にありません: {eid} -> {th}")
            if kind in STATE_CHANGING:
                raise RTError(f"新規threadを{kind}から開始できません: {eid}")
            expected_status = _TYPE_STATUS.get(kind) or "open"
            claimant = None
            states[th] = {"status": expected_status, "claimant": claimant}
        else:
            status = state["status"]
            claimant = state["claimant"]
            if status == "complete" and kind != "note":
                raise RTError(f"完了済みthreadへの{kind}を検出: {th}")
            if kind == "claim":
                if claimant and claimant != ev["sender"]:
                    raise RTError(
                        f"二重claimを検出: {th} {claimant} / {ev['sender']}")
                claimant = ev["sender"]
                status = "claimed"
            elif kind in ("progress", "blocked", "complete"):
                if not claimant or claimant != ev["sender"]:
                    raise RTError(
                        f"担当外の{kind}を検出: {th} claimant={claimant} "
                        f"sender={ev['sender']}")
                status = _TYPE_STATUS[kind]
            # instruction/question/answer/noteは既存workflow状態を変えない。
            expected_status = status
            states[th] = {"status": status, "claimant": claimant}

        # 旧Markdown移行event（payload_hash未導入世代）はquestion/noteで
        # statusを書き戻す旧規則だった。正本は改変せずgrandfatherし、
        # payload_hash付きの新writer世代だけstored statusまで厳格照合する。
        modern_writer = bool(ev.get("payload_hash"))
        if modern_writer and ev["status"] != expected_status:
            raise RTError(
                f"event.statusがreplay結果と不一致: {eid} "
                f"stored={ev['status']} expected={expected_status}")
        event_threads[eid] = th


def _terminal_winner_unlocked(rt, sender: str, reply_to: str):
    """同一作業(reply_to)+担当の最初の終端をJSONL正本から返す。

    呼出側はevents排他lockを保持する。SQLite索引ではなく追記正本を見ることで、
    索引更新失敗直後でも blocked/complete の二重記録を許さない。
    """
    winners = [
        ev for ev in _iter_events_unlocked(rt, _log=False)
        if (ev.get("sender") == sender and ev.get("reply_to") == reply_to
            and ev.get("type") in ("blocked", "complete"))
    ]
    if not winners:
        return None
    kinds = {ev.get("type") for ev in winners}
    if len(kinds) != 1:
        raise RTError(
            f"終端正本が競合しています: sender={sender} reply_to={reply_to} "
            f"types={','.join(sorted(kinds))}")
    return winners[0]


def _claim_reply_to(con, thread_id: str | None, reply_to: str | None) -> str:
    """claimを指示eventへ結ぶ。複数候補を話題単位で曖昧にclaimしない。"""
    if reply_to or not thread_id:
        return reply_to or ""
    instructions = [row[0] for row in con.execute(
        "SELECT event_id FROM events WHERE thread_id=? AND type='instruction' ORDER BY seq",
        (thread_id,)).fetchall()]
    if not instructions:
        return ""
    claimed = {row[0] for row in con.execute(
        "SELECT reply_to FROM events WHERE thread_id=? AND type='claim' AND reply_to<>''",
        (thread_id,)).fetchall()}
    open_instructions = [event_id for event_id in instructions if event_id not in claimed]
    if len(open_instructions) == 1:
        return open_instructions[0]
    if len(instructions) == 1:
        return instructions[0]
    if not open_instructions:
        raise RTError(f"未着手の指示がありません: thread={thread_id}")
    raise RTError(
        f"複数の未着手指示があります。claimにはreply_toを指定してください: "
        f"thread={thread_id} candidates={','.join(open_instructions)}")


# ---------------- 追記（emit本体・逐次索引維持） ----------------

def _owari_no_annai(topic) -> str:
    """完了済みthreadを指された時に、**実際に通る手だけ**を書いて返す。

    2026-09-05 修理(バグ探し中#8): 従来の文面は『新threadで: <完了済みthread_id>』。
    読んだ側はそのIDを RT_THREAD_ID に入れて再投稿し、同じ終端ガードで弾かれる。
    完了済みIDを渡す限り何度やっても通らない(2026-09-04にClaudeが4回踏んだ)。
    高#5の修理で「同じ話題名でも新しい糸が立つ」ようになったので、通る手は
    『thread_idの指定を外し、話題名を指定して投稿する』の一手だけ。IDは出さない。
    """
    saki = f"話題『{topic}』" if topic and str(topic).strip() else "話題名"
    return ("この糸は完了しています。thread_idの指定を外し、"
            f"{saki}を指定して投稿すれば新しい糸が立ちます"
            "（claim/progress/complete は、その新しい糸に対して行う）")


def emit(rt, *, sender, to, content, type="note", thread_id=None, reply_to=None,
         report_path="", dedup_key=None, source="v2", at_day=None, at_hm=None,
         terminal_once=False, topic=None, proof="未検証", request_id=None,
         projection_required=False, mado="") -> dict:
    sender = (sender or "").strip()
    to = (to or "").strip() or "全員"
    content = content if content is not None else ""
    type = (type or "note").strip()
    topic_unspecified = topic is None or not str(topic).strip()
    topic = "" if topic_unspecified else _normalize_topic(str(topic))
    proof = (proof or "未検証").strip()
    request_id = (request_id or "").strip()

    if not valid_sender(sender):
        raise RTError(f"差出人の名前が使えません: {sender!r}（空・40字超・制御文字・記号）")
    if type not in TYPES:
        raise RTError(f"typeが不正です: {type!r}（{'/'.join(TYPES)}）")
    if proof not in PROOFS:
        raise RTError(f"実証度が不正です: {proof!r}（{'/'.join(PROOFS)}）")
    if proof == "王合格" and sender != "王":
        raise RTError("王合格を記録できるのは王だけです")
    if request_id and (len(request_id) > 200 or re.search(r"[\x00-\x20\x7f]", request_id)):
        raise RTError("request_idは1〜200文字の非空白・非制御文字にしてください")
    # 正本でも制御文字とMarkdown発言見出し偽装を無害化する。
    content = "".join(ch for ch in content
                      if ch in "\n\t" or (ord(ch) >= 32 and ord(ch) != 127))
    if not content.strip():
        raise RTError("空の発言は置けません")
    content = redact_secrets(content)  # 保存前伏字
    content = re.sub(r"(?m)^(##+ \[)", r"> \1", content)
    # 派生Markdownの再投影markerを本文から偽造させない。
    content = re.sub(r"<!--\s*rt:event:", "&lt;!-- rt:event:", content,
                     flags=re.IGNORECASE)
    if len(content.encode("utf-8")) > MAX_CONTENT_BYTES:
        raise RTError(f"本文が64KB上限を超えています（{len(content.encode('utf-8'))}バイト）")
    recipients = _parse_recipients(to)
    _validate_recipients(recipients)
    report_path = _clean_report_path(report_path)
    if not report_path:
        # 本人が本文に書いた証拠を欄へ写す(空の時だけ・実在する候補が1つの時だけ)
        report_path = _clean_report_path(honbun_kara_shouko(rt, content))
    if type == "claim" and not _is_rt_worker(sender):
        raise RTError(f"{sender} は作業をclaimできません（実務参加者のみ）")

    _ensure_dirs(rt)
    with _clean_events_exclusive(rt):
        try:
            con = _connect(rt)
            try:
                iso, day, ns = _now_parts()
                hm = iso[11:16]
                if bool(at_day) != bool(at_hm):
                    raise RTError("at_dayとat_hmは両方を指定してください")
                if at_day and at_hm:
                    if not re.fullmatch(r"\d{8}", at_day) or not re.fullmatch(r"\d{2}:\d{2}", at_hm):
                        raise RTError("at_day/at_hmの形式が不正です")
                    try:
                        time.strptime(at_day + at_hm, "%Y%m%d%H:%M")
                    except ValueError as exc:
                        raise RTError(f"at_day/at_hmが実在しない日時です: {exc}") from exc
                    day, hm = at_day, at_hm
                    iso = f"{day[:4]}-{day[4:6]}-{day[6:8]}T{hm}:00" + iso[19:]

                if type == "claim":
                    reply_to = _claim_reply_to(con, thread_id, reply_to)

                inherited_topic_slug = ""
                if topic_unspecified:
                    topic, inherited_topic_slug = _infer_topic_binding_for_thread(
                        con, rt, thread_id, reply_to)

                # dedupはキーだけでなく、全入力payloadの一致も必須。
                # これにより同一本文でもtype/topic/proof/threadが違えば統合されない。
                request_payload = {
                    "sender": sender, "to": to, "recipients": recipients, "type": type,
                    "content": content, "thread_id": thread_id or "", "reply_to": reply_to or "",
                    "report_path": report_path or "", "topic": topic, "proof": proof,
                    "source": source,
                    "projection_required": bool(projection_required),
                    # request_idは配送再試行のID。再試行時刻はpayloadに含めない。
                    "at_day": "" if request_id else (at_day or ""),
                    "at_hm": "" if request_id else (at_hm or ""),
                }
                ph = _payload_hash(request_payload)
                if request_id:
                    dk = "req:" + hashlib.sha256(request_id.encode("utf-8")).hexdigest()
                elif source == "markdown":
                    dk = dedup_key or _markdown_dedup_key(ph)
                else:
                    dk = _explicit_dedup(dedup_key, sender, to, content, day, hm)
                # 明示/Markdownキーの既存確認は索引で O(log n)。
                if source == "markdown" or dedup_key or request_id:
                    ex = con.execute("SELECT event_id,thread_id,payload_hash FROM events WHERE dedup_key=?",
                                     (dk,)).fetchone()
                    if ex:
                        if not ex[2]:
                            raise RTError(
                                f"旧式dedupキーは完全payloadを検証できないため再利用不可: {dk}")
                        if ex[2] != ph:
                            raise RTError(f"dedup_keyは既存ですがpayloadが異なります: {dk}")
                        existing = next(
                            (e for e in _iter_events_unlocked(rt, _log=False)
                             if e.get("event_id") == ex[0]), None)
                        if existing is None:
                            raise RTError(f"dedup索引の指すJSONL正本がありません: {ex[0]}")
                        existing = dict(existing)
                        existing.pop("_file", None); existing.pop("_lineno", None)
                        existing["_dedup"] = True
                        return existing
                    if source == "markdown" and not dedup_key and not request_id:
                        # 旧md:キーは項目が少ない。旧時点の暗黙値も含め全項目を
                        # 照合できるbackfill(note/雑談/未検証)のみ既存eventを再利用する。
                        old_dk = _legacy_markdown_dedup_key(sender, to, content, day, hm)
                        old = con.execute(
                            'SELECT event_id,thread_id,sender,"to",type,content,report_path,source '
                            'FROM events WHERE dedup_key=?', (old_dk,)).fetchone()
                        if old:
                            legacy_exact = (
                                old[2] == sender and old[3] == to and old[4] == "note"
                                and old[5] == content and not old[6]
                                and type == "note" and topic == "雑談" and proof == "未検証"
                                and not thread_id and not reply_to and not report_path)
                            if not legacy_exact:
                                raise RTError(
                                    f"旧dedupキーと衝突しましたがpayloadが異なります: {old_dk}")
                            existing = next(
                                (e for e in _iter_events_unlocked(rt, _log=False)
                                 if e.get("event_id") == old[0]), None)
                            if existing is None:
                                raise RTError(f"旧dedup索引のJSONL正本がありません: {old[0]}")
                            existing = dict(existing)
                            existing.pop("_file", None); existing.pop("_lineno", None)
                            existing["_dedup"] = True
                            existing["_legacy_dedup"] = True
                            return existing

                if inherited_topic_slug:
                    topic_slug = inherited_topic_slug
                elif topic_unspecified and not thread_id and not reply_to:
                    # topic省略APIは後方互換で毎回new thread。その全てを「雑談」
                    # 1slugへ結ぶと、後日のclaim投影が.thread_mapで交差する。
                    topic_slug = (
                        f"{_legacy_topic_slug(topic)}--thread-{uuid.uuid4().hex[:12]}")
                else:
                    topic_slug = _resolve_topic_slug(con, rt, topic)

                # thread解決。thread_id指定時もreply_toの所属を必ず照合する。
                th = thread_id
                if reply_to:
                    row = con.execute("SELECT thread_id FROM events WHERE event_id=?",
                                      (reply_to,)).fetchone()
                    if not row:
                        raise RTError(f"reply_to先が見つかりません: {reply_to!r}")
                    if th is not None and th != row[0]:
                        raise RTError(
                            f"reply_toとthread_idが一致しません: {reply_to!r}")
                    th = row[0]
                # 2026-09-05 修理(バグ探し高#5): 「1話題=1糸」→「1話題=1本の生きている糸」。
                #   束ね先は「話題の最初の糸」ではなく「生きている最新の糸」。全部完了していたら
                #   状態を変える型は新糸を立て、note は最後の糸へ追記(今までどおり)。
                topic_family = ({tid for tid, _st in _topic_threads(con, topic, topic_slug)}
                                if not topic_unspecified else set())
                topic_binding = (_live_topic_thread(con, topic, topic_slug, type)
                                 if not topic_unspecified else None)
                if th is None:
                    # topicを明示した投稿だけ話題の糸で結ぶ。
                    # 古いevent APIの topic省略は従来どおり毎回新threadとし、
                    # 既定値「雑談」の完了threadに全作業を押し込まない。
                    if topic_binding and not topic_unspecified:
                        th = topic_binding
                    elif not topic_unspecified and not topic_family:
                        # DBに糸が無い話題だけ、旧対応表から拾う(schema拡張前の糸)
                        mp = rt / "話題" / ".thread_map"
                        if mp.exists():
                            try:
                                for line in mp.read_text(encoding="utf-8", errors="replace").splitlines():
                                    parts = line.split("\t", 1)
                                    if len(parts) == 2 and parts[0] == topic_slug:
                                        th = parts[1].strip() or None
                            except OSError:
                                pass
                        if th is not None:
                            # 2026-09-05 修理(バグ探し低#9): 旧対応表149行のうち**14行**は、
                            #   その話題のeventを正本に1件も持たない糸を指していた
                            #   (2026-07-29のtopicが空だった世代の糸)。信じて拾うと、
                            #   『動画作成の仕組み』『写真検品ツール』などの話題名で投稿した物が
                            #   まったく無関係な古い糸へ黙って接ぎ木される。しかも
                            #   「明示threadを別の話題へ繋ぐな」の検査は thread_id を
                            #   明示した枝にしか無いので、この道は素通りする。
                            #   拾った糸が本当に話題を名乗った事があるかを正本で確かめる。
                            #   (実測: 名乗っていない糸=14行、名乗っている糸=135行で丁度分かれる)
                            if _map_no_furui_ito(con, {th}):
                                th = None   # 話題を一度も名乗っていない糸は、この話題の糸ではない
                        if th is not None and _TYPE_STATUS.get(type) is not None:
                            _ex_mp, _st_mp, _cl_mp = _thread_state(con, th)
                            if _ex_mp and _st_mp in _DEAD_STATUSES:
                                th = None   # 旧対応表の糸が終わっていたら新糸
                else:
                    if not topic_unspecified and topic_family and th not in topic_family:
                        raise RTError(
                            f"話題は別threadに結び付いています: "
                            f"topic={topic!r} threads={','.join(sorted(topic_family))}")
                    # 明示threadを別の話題に接続してはならない。
                    bound = con.execute(
                        "SELECT topic,topic_slug FROM events WHERE thread_id=? "
                        "AND topic_slug<>'' ORDER BY seq LIMIT 1", (th,)).fetchone()
                    if bound and (bound[0] != topic or bound[1] != topic_slug):
                        safe_legacy_rebind = (
                            topic_unspecified
                            and _safe_inferred_legacy_rebind(
                                con, rt, th, bound[1], topic_slug))
                        if not safe_legacy_rebind:
                            raise RTError(
                                f"thread_idは別話題に属します: thread={th} "
                                f"topic={bound[0]!r}/{bound[1]!r}")
                    if not bound and not topic_unspecified:
                        mp = rt / "話題" / ".thread_map"
                        if mp.exists():
                            try:
                                mapped_slugs = {
                                    p[0] for line in mp.read_text(
                                        encoding="utf-8", errors="replace").splitlines()
                                    if len(p := line.split("\t", 1)) == 2 and p[1].strip() == th
                                }
                            except OSError:
                                mapped_slugs = set()
                            if mapped_slugs and topic_slug not in mapped_slugs:
                                raise RTError(
                                    f"thread_idは別slugに属します: thread={th} "
                                    f"slugs={','.join(sorted(mapped_slugs))}")
                    exists_explicit, _st_explicit, _claim_explicit = _thread_state(con, th)
                    if not exists_explicit:
                        raise RTError(f"存在しないthread_idは指定できません: {th}")

                # 旧.thread_mapに重複や交差がある場合も、JSONL追記前にfail-closed。
                # canonicalと古い対応表が違うまま、投影だけを別threadへ残さない。
                if not topic_unspecified:
                    mp = rt / "話題" / ".thread_map"
                    map_pairs = []
                    if mp.exists():
                        try:
                            map_pairs = [
                                (p[0], p[1].strip())
                                for line in mp.read_text(
                                    encoding="utf-8", errors="replace").splitlines()
                                if len(p := line.split("\t", 1)) == 2 and p[0] and p[1].strip()
                            ]
                        except OSError as exc:
                            raise RTError(f".thread_mapを検証できません: {exc}") from exc
                    slug_threads = {mapped_th for mapped_slug, mapped_th in map_pairs
                                    if mapped_slug == topic_slug}
                    # 2026-09-05 低#9: 話題を一度も名乗っていない古い糸は判定から外す
                    slug_threads -= _map_no_furui_ito(con, slug_threads)
                    # 2026-09-05 修理(バグ探し高#5): 同じ話題の糸が複数あるのは正常。
                    #   別話題・topic空の糸が混じる時だけ「交差」。
                    if len(slug_threads) > 1 and not _threads_all_in_topic(
                            con, slug_threads, topic, topic_slug):
                        raise RTError(
                            f".thread_mapでslugが複数threadに交差: {topic_slug} -> "
                            f"{','.join(sorted(slug_threads))}")
                    if th and slug_threads and th not in slug_threads and not _threads_all_in_topic(
                            con, slug_threads | {th}, topic, topic_slug):
                        raise RTError(
                            f".thread_mapとcanonical threadが不一致: "
                            f"{topic_slug} -> {','.join(sorted(slug_threads))} / {th}")
                    if th:
                        thread_slugs = {mapped_slug for mapped_slug, mapped_th in map_pairs
                                        if mapped_th == th}
                        if len(thread_slugs) > 1:
                            raise RTError(
                                f".thread_mapでthreadが複数slugに交差: {th} -> "
                                f"{','.join(sorted(thread_slugs))}")
                        if thread_slugs and topic_slug not in thread_slugs:
                            raise RTError(
                                f".thread_mapとcanonical slugが不一致: "
                                f"{th} -> {','.join(sorted(thread_slugs))} / {topic_slug}")
                new_thread = th is None

                # 日別JSONLはファイル名順、同日内は追記順でreplayする。
                # 既存threadへ過去日を追記するとrootより先に読まれ得るため、
                # 正本へ書く前にthread内の日付後退だけを拒否する。同日内は
                # timestampではなくappend順が正本順なので許可する。
                if not new_thread:
                    newer = con.execute(
                        "SELECT timestamp,event_id FROM events "
                        "WHERE thread_id=? AND substr(timestamp,1,10)>? "
                        "ORDER BY substr(timestamp,1,10) DESC,seq DESC LIMIT 1",
                        (th, iso[:10])).fetchone()
                    if newer:
                        raise RTError(
                            f"既存threadへ過去日付は追記できません: "
                            f"thread={th} newer={newer[0]}/{newer[1]} requested={iso}")

                # 自動終端は同一担当・同一reply_toにつき最初の1件だけ。
                # events排他lock内のJSONL正本CASなので、workerと回収workerが競争しても
                # blocked→complete（または逆）の矛盾を追記できない。
                if terminal_once:
                    if type not in ("blocked", "complete") or not reply_to:
                        raise RTError("terminal_onceにはblocked/completeとreply_toが必要です")
                    winner = _terminal_winner_unlocked(rt, sender, reply_to)
                    if winner is not None:
                        existing = dict(winner)
                        existing.pop("_file", None)
                        existing.pop("_lineno", None)
                        existing["_dedup"] = True
                        existing["_terminal_winner"] = True
                        return existing

                # complete は終端（Codex P1-D）: 完了済みthreadへ状態を変える型を全拒否。
                # note（status変えない）だけは追記可（再openさせない）。
                # ★2026-11-15 王命再発防止（道2）: 完了済みthreadであっても、
                #   未受領の指示の宛先本人による救済受領（claim/answer）は例外として許可し、再開させる。
                is_rescue = False
                if not new_thread:
                    ex0, st0, _cl0 = _thread_state(con, th)
                    if ex0 and st0 == "complete" and _TYPE_STATUS.get(type) is not None:
                        if type in ("claim", "answer") and _is_unclaimed_instruction_rescue(con, th, sender, reply_to):
                            is_rescue = True
                        else:
                            raise RTError(f"完了済みthreadは終端です（{type}で再openできません）。"
                                                f"{_owari_no_annai(topic)}")

                # 状態遷移の厳格検証（Codex P0-2）
                if type == "claim":
                    if new_thread:
                        raise RTError("claimには既存のthread_idが必要です（存在しないthreadはclaim不可）")
                    exists, status, claimant = _thread_state(con, th)
                    if not exists:
                        raise RTError(f"存在しないthreadはclaimできません: {th}")
                    if status == "complete":
                        if not is_rescue:
                            raise RTError(f"完了済みthreadはclaimできません。{_owari_no_annai(topic)}")
                    elif claimant and claimant != sender:
                        is_handoff = (status == "blocked") or any(w in content for w in ("引き継ぎ", "引継ぎ", "引き継ぐ", "交代", "handoff"))
                        if not is_handoff:
                            raise RTError(f"二重claim拒否: thread={th} は {claimant} がclaim中（{sender}は不可。引き継ぐ場合は内容に「引き継ぎ」と明記するか、前担当者がblockedの時にclaimしてください）")
                elif type in ("progress", "blocked", "complete"):
                    if new_thread:
                        raise RTError(f"{type}には既存のthread_idが必要です")
                    exists, status, claimant = _thread_state(con, th)
                    if not exists:
                        raise RTError(f"存在しないthreadに{type}はできません: {th}")
                    if status == "complete":
                        raise RTError(f"完了後の変更はできません（thread={th} は既にcomplete）。"
                                       f"{_owari_no_annai(topic)}")
                    if claimant is None:
                        raise RTError(f"未claimのthreadに{type}はできません（先にclaimが必要）: {th}")
                    if claimant != sender:
                        raise RTError(f"不正遷移: thread={th} の担当は {claimant}。{sender} は {type} 不可（先に内容に「引き継ぎ」を含めて claim して担当を引き継いでください）")
                    # ★2026-11-15 王命再発防止（道1）: 他人宛ての未受領指示が残るスレッドは complete できない
                    if type == "complete":
                        unclaimed = _find_unclaimed_instructions_for_others(con, th, sender)
                        if unclaimed:
                            u_ev = unclaimed[0]
                            raise RTError(f"未受領の指示が残っているためcompleteできません: event_id={u_ev['event_id']} 宛先={u_ev['to']}（相手がclaim/受領する前にスレッドを閉じることは禁じられています。相手の受領を待つか、不要なら指示を取り消してください）")

                if new_thread or type in STATE_CHANGING:
                    st = _TYPE_STATUS.get(type) or "open"
                else:
                    _exists_now, current_status, _claim_now = _thread_state(con, th)
                    st = current_status or "open"
                event_id = _make_event_id(ns)
                if new_thread:
                    th = event_id

                # Evaluate only at first insertion; idempotent retries return the
                # original event above even if evidence has since aged or changed.
                from claim_evidence import assess_post
                assessment = assess_post(Path(rt).parent, content, sender, mado, proof)
                ev = {
                    "schema_version": SCHEMA_VERSION, "event_id": event_id, "thread_id": th,
                    "reply_to": reply_to or "", "timestamp": iso, "sender": sender, "to": to,
                    "recipients": recipients, "type": type, "status": st, "content": content,
                    "report_path": report_path or "", "dedup_key": dk, "source": source,
                    "request_id": request_id,
                    "topic": topic, "proof": assessment["proof"], "topic_slug": topic_slug,
                    "proof_requested": proof, "proof_assessment": assessment,
                    "payload_hash": ph,
                    "projection_required": bool(projection_required),
                }
                if mado:
                    # 窓の名(任意・案件 jiku5f)。emit は _valid_event を通さず書くので、ここで清める
                    # (禁止文字を落として40字。壊れた値を正本へ書くと rebuild で bad_mado 隔離になる)
                    _m = _SENDER_NG.sub("", str(mado))[:40].strip()
                    if _m:
                        ev["mado"] = _m
                # post pipelineはJSONL受理直後からMarkdown/人間向け索引の投影が
                # 完了するまで、この耐久markerを保持する。JSONL追記後かつ
                # materialize開始前のkill窓も次回投稿が確実に回復できる。
                _assert_all_jsonl_tails_complete_unlocked(rt)
                if projection_required:
                    _write_marker(
                        _views_dirty_path(rt) / event_id,
                        "precommit\n", required=True)
                # 1) DB未反映markerを先に耐久化し、JSONL正本へ追記。
                # JSONL fsync後〜SQLite commit前に強制終了しても、
                # 次操作はmarkerを見てJSONLからrebuildする。
                _set_dirty(rt, f"pending:{event_id}\n", required=True)
                fpath = _events_dir(rt) / f"events_{day}.jsonl"
                with fpath.open("a", encoding="utf-8") as out:
                    out.write(json.dumps(ev, ensure_ascii=False, separators=(",", ":")) + "\n")
                    out.flush()
                    os.fsync(out.fileno())
                try:
                    os.chmod(fpath, 0o600)   # data 600（新旧問わず・Codex cond7）
                except OSError:
                    pass
                _fsync_dir(_events_dir(rt))
                # 2) 索引へ逐次反映。失敗時はdirty markerを立て、次操作で自動rebuild（Codex P1-E）
                try:
                    if not _db_apply(con, ev):
                        raise RuntimeError("dedup一意制約により新eventの索引反映が拒否されました")
                    con.commit()
                    _clear_dirty(rt)
                except Exception as exc:
                    _set_dirty(rt, f"dirty:{event_id}:{exc.__class__.__name__}\n")
                    ev["_index_dirty"] = True
                    ev["_index_error"] = f"{exc.__class__.__name__}: {exc}"
                else:
                    # 書き込みの関（AUDNの門・王命_20260909-1129 段1）。
                    # 頭脳へ1件入る瞬間に 足す/直す/何もしない の札を決定的に付ける。
                    # ここが唯一の門（入口は emit 1本。出口ごとには書かない）。
                    # 落ちても投稿は殺さない（円卓のeventは証拠でappend-only）が、
                    # 黙りもしない ―― 迷い箱と _seki に必ず残す。
                    try:
                        import kakikomi_no_seki as _KS
                        ev["_seki"] = _KS.toosu(con, rt, ev)
                        con.commit()
                    except BaseException as exc:
                        # BaseException まで握る理由（2026-09-09 ネオ ai-085a の検分）:
                        #   KeyboardInterrupt/SystemExit がここから飛ぶと、JSONL正本は
                        #   既に fsync 済みで**投稿は成立しているのに**呼び手が失敗と誤解し、
                        #   投げ直して**円卓に二重に載る**（実測 1件→2件）。
                        #   門の失敗で投稿の成否を覆さない。門は数ミリ秒なので、
                        #   ここで Ctrl-C を握っても実害は「札が付かない」だけ。
                        ev["_seki"] = {"kettei": "shippai",
                                       "riyuu": f"{exc.__class__.__name__}: {exc}",
                                       "mayoi": True}
                        # import が落ちた時は門の中の記録係も動かない＝失敗が
                        # どこにも残らず、門が黙って効かなくなる。**素の追記で残す。**
                        try:
                            _m = rt / "brain" / "mayoi.jsonl"
                            _m.parent.mkdir(parents=True, exist_ok=True)
                            with _m.open("a", encoding="utf-8") as _f:
                                _f.write(json.dumps(
                                    {"schema": "kingdom.roundtable.kakikomi_seki.v1",
                                     "kettei": "shippai", "mayoi": True,
                                     "event_id": ev.get("event_id", ""),
                                     "riyuu": ev["_seki"]["riyuu"], "at": iso,
                                     "doko": "emit側(門のimport失敗か、門が握れなかった例外)"},
                                    ensure_ascii=False) + "\n")
                        except OSError:
                            pass
                return ev
            finally:
                con.close()
        finally:
            pass


def _find_event(rt: Path, event_id: str) -> dict:
    if not _EVENT_ID_RE.fullmatch(event_id or ""):
        raise RTError(f"event_idの形式が不正です: {event_id!r}")
    with _events_lock(rt).open("w") as elk:
        fcntl.flock(elk, fcntl.LOCK_SH)
        try:
            for ev in _iter_events_unlocked(rt, _log=False):
                if ev.get("event_id") == event_id:
                    out = dict(ev)
                    out.pop("_file", None); out.pop("_lineno", None)
                    return out
        finally:
            fcntl.flock(elk, fcntl.LOCK_UN)
    raise RTError(f"event_idがJSONL正本に見つかりません: {event_id}")


def _append_fsync(path: Path, text: str) -> None:
    with path.open("a", encoding="utf-8") as out:
        out.write(text)
        out.flush()
        os.fsync(out.fileno())
    try:
        os.chmod(path, 0o600)
    except OSError:
        pass
    _fsync_dir(path.parent)


def _finish_initial_header(path: Path, header: str) -> None:
    """初回headerのpartial writeだけをappendで完成させる。"""
    expected = header.encode("utf-8")
    current = path.read_bytes() if path.exists() else b""
    if b"\n" in current:
        return
    if not expected.startswith(current):
        raise RTError(f"派生Markdownの先頭が不正です（自動上書きしません）: {path}")
    if len(current) < len(expected):
        with path.open("ab") as out:
            out.write(expected[len(current):])
            out.flush()
            os.fsync(out.fileno())
        _fsync_dir(path.parent)


def _append_recoverable_block(path: Path, block: str, begin_marker: str,
                              complete_marker: str) -> bool:
    """partial appendを上書きせず、未書込みsuffixだけ追記して完成させる。"""
    current = path.read_bytes() if path.exists() else b""
    complete = complete_marker.encode("utf-8")
    if complete in current:
        return False
    expected = block.encode("utf-8")
    # crash時点のfile末尾と、予定block先頭の最長一致を採用する。
    # これによりUTF-8文字の途中で止まっていてもbyte単位で安全に継続できる。
    overlap = 0
    for size in range(min(len(current), len(expected)), 0, -1):
        if current.endswith(expected[:size]):
            overlap = size
            break
    begin = begin_marker.encode("utf-8")
    if begin in current and overlap == 0:
        raise RTError(
            f"投影blockの途中に予期しない追記があります。自動重複を避け停止: {path}")
    with path.open("ab") as out:
        out.write(expected[overlap:])
        out.flush()
        os.fsync(out.fileno())
    try:
        os.chmod(path, 0o600)
    except OSError:
        pass
    _fsync_dir(path.parent)
    return True


def _finish_expected_map_tail(path: Path, expected_line: str) -> None:
    """.thread_mapの末尾partialが今回eventの行ならsuffixだけを追記する。"""
    if not path.exists() or path.stat().st_size == 0:
        return
    current = path.read_bytes()
    if current.endswith(b"\n"):
        return
    tail = current.rsplit(b"\n", 1)[-1]
    expected = expected_line.encode("utf-8")
    if not expected.startswith(tail):
        raise RTError(
            f".thread_mapが未知の途中行で終わっています。自動上書きせず停止: {path}")
    with path.open("ab") as out:
        out.write(expected[len(tail):])
        out.flush()
        os.fsync(out.fileno())
    _fsync_dir(path.parent)


def materialize_markdown(rt: Path, event_id: str, *, defer_clear: bool = False) -> dict:
    """JSONL正本の1eventをMarkdown派生表示へ再安全に追記する。

    event_id markerで各投影を冪等化し、途中失敗後の再実行で足りない側だけ回復する。
    過去行の編集・削除はしない。
    """
    ev = _find_event(rt, event_id)
    topic = _normalize_topic(ev.get("topic", "雑談"))
    proof = ev.get("proof", "未検証")
    slug = ev.get("topic_slug") or _legacy_topic_slug(topic)
    ts = ev["timestamp"]
    day = ts[:10].replace("-", "")
    hm = ts[11:16]
    marker_hash = hashlib.sha256(
        (event_id + "\x1f" + ev.get("payload_hash", "") + "\x1f"
         + ev.get("content", "")).encode("utf-8")).hexdigest()[:16]
    # complete markerはblockの末尾に書く。markerまで無い中途書きは
    # 再試行対象となり、本文内の偽造markerはemit時に無害化済み。
    begin_marker = f"<!-- rt:event:{event_id}:begin:{marker_hash} -->"
    marker = f"<!-- rt:event:{event_id}:complete:{marker_hash} -->"
    topic_dir = rt / "話題"
    topic_dir.mkdir(parents=True, exist_ok=True)
    topic_file = topic_dir / f"{slug}.md"
    daily_file = rt / f"円卓_{day}.md"
    body = ev.get("content", "")
    # 2026-08-22: 60字では「【要点】」と宣言した直後で切れて要点が読めず、
    #   結局 roundtable_thread を叩き直すので往復が倍になっていた。
    #   150字にした理由: 一覧は「粗い一覧」のままにしたい(詳細はthreadで読む設計)。
    #   300字にすると n=20 で6000字を超え、一覧と詳細の役割分担が崩れる。
    #   ⚠ 円卓mdは追記のみなので、**過去の発言は60字のまま**。効くのは今日から先だけ。
    gist = body.replace("\n", " ")[:150]
    dirty_dir = _views_dirty_path(rt)
    dirty_marker = dirty_dir / event_id
    result = {"event_id": event_id, "topic": topic, "topic_slug": slug,
              "thread_id": ev["thread_id"], "topic_written": False,
              "daily_written": False, "dedup": bool(ev.get("_dedup"))}

    # 最初のMarkdown追記より先にmarkerをfsync。kill/crashで中断しても
    # event_id markerを使った再投影の対象が消えない。
    _write_marker(dirty_marker, "pending\n", required=True)
    try:
        with _views_lock(rt).open("w") as lk:
            fcntl.flock(lk, fcntl.LOCK_EX)
            try:
                # 対応表の不整合はMarkdownを1byteも追記する前に拒否する。
                mp = topic_dir / ".thread_map"
                expected_map_line = f"{slug}\t{ev['thread_id']}\n"
                _finish_expected_map_tail(mp, expected_map_line)
                mapping = (mp.read_text(encoding="utf-8", errors="replace").splitlines()
                           if mp.exists() else [])
                pairs = [
                    (p[0], p[1].strip()) for line in mapping
                    if len(p := line.split("\t", 1)) == 2 and p[0] and p[1].strip()
                ]
                slug_threads = {mapped_th for mapped_slug, mapped_th in pairs
                                if mapped_slug == slug}
                thread_slugs = {mapped_slug for mapped_slug, mapped_th in pairs
                                if mapped_th == ev["thread_id"]}
                if slug_threads and slug_threads != {ev["thread_id"]}:
                    # 2026-09-05 修理(バグ探し高#5): 同じ話題の糸が複数なら不一致ではない
                    _con_ro = sqlite3.connect(f"file:{_db_path(rt)}?mode=ro", uri=True, timeout=10)
                    try:
                        # 2026-09-05 低#9: 旧対応表が指す「話題を名乗っていない糸」は
                        #   照合相手から外す(外さないと、正しく新糸を立てた投稿の投影が落ちる)
                        _furui = _map_no_furui_ito(_con_ro, slug_threads)
                        _same = _threads_all_in_topic(
                            _con_ro, (slug_threads - _furui) | {ev["thread_id"]}, topic, slug)
                    finally:
                        _con_ro.close()
                    if not _same:
                        raise RTError(
                            f".thread_mapとeventのthreadが不一致: "
                            f"{slug} -> {','.join(sorted(slug_threads))} / {ev['thread_id']}")
                if thread_slugs and thread_slugs != {slug}:
                    raise RTError(
                        f".thread_mapとeventのslugが不一致: "
                        f"{ev['thread_id']} -> {','.join(sorted(thread_slugs))} / {slug}")

                topic_header = f"# 話題: {topic}（追記のみ・掟は ../掟.md）\n"
                if topic_file.exists():
                    _finish_initial_header(topic_file, topic_header)
                    existing_topic = _topic_header(topic_file)
                    if not existing_topic:
                        raise RTError(f"話題ファイルのヘッダが不正です: {topic_file}")
                    if existing_topic != topic:
                        raise RTError(
                            f"topic_slug衝突を投影時に検出: {slug} "
                            f"({existing_topic!r} != {topic!r})")
                    topic_text = topic_file.read_text(encoding="utf-8", errors="replace")
                else:
                    _append_fsync(topic_file, topic_header)
                topic_block = (
                    f"\n{begin_marker}\n"
                    f"## [{day} {hm}] {ev['sender']} → {ev.get('to', '全員')} "
                    f"〔{ev['type']}/{proof}〕\n{body}\n{marker}\n")
                if _append_recoverable_block(
                        topic_file, topic_block, begin_marker, marker):
                    result["topic_written"] = True

                daily_header = (
                    f"# 王国円卓 {day}（目次・本文は 話題/ の各1枚・掟は roundtable/掟.md）\n")
                if daily_file.exists():
                    _finish_initial_header(daily_file, daily_header)
                if not daily_file.exists():
                    _append_fsync(daily_file, daily_header)
                daily_block = (
                    f"\n{begin_marker}\n"
                    f"## [{hm}] {ev['sender']} → {ev.get('to', '全員')}\n"
                    f"→ 話題/{slug}.md 〔{topic}/{ev['type']}/{proof}〕 {gist}\n"
                    f"{marker}\n")
                if _append_recoverable_block(
                        daily_file, daily_block, begin_marker, marker):
                    result["daily_written"] = True

                if ev["thread_id"] not in slug_threads:
                    # 2026-09-05 修理(バグ探し高#5): 2本目以降の糸も対応表へ(追記のみ)
                    _append_fsync(mp, expected_map_line)
            finally:
                fcntl.flock(lk, fcntl.LOCK_UN)
    except Exception as exc:
        _write_marker(dirty_marker, f"{exc.__class__.__name__}: {exc}\n")
        raise
    if not defer_clear:
        clear_projection_marker(rt, event_id)
    return result


def _views_quarantine_path(rt): return rt / ".views_quarantine"


def recover_projections(rt: Path, *, run_index: bool = True, index_script: Path | None = None) -> dict:
    """.views_dirty に残った未投影markerを回収する。post.sh の回復ブロックと同じ処理を、ここ1箇所に置く。
    2026-09-05 修理(バグ探し中#10・低#10、くくる提案P1の最小形):
      中#10: 5分ごとの refresh は未投影を数えるだけで直さず、次に誰かが投稿するまで話題mdに載らなかった
             → refresh(roundtable_loop.py) からもこれを呼ぶ。
      低#10: 回収が1件でも恒久的に失敗すると post.sh が exit 13 で止まり、以後全員が投稿できなくなった
             → 失敗した marker は .views_quarantine/<event_id> へ退避して警告1行。正本(JSONL)は確定済みなので、
               派生(Markdown)の失敗で投稿口を閉じない。世界の定石(読み模型は遅れる前提・壊れた1件で口を止めない)。
    返り: recovered(投影し直したid) / stale_cleared(正本に無いmarkerを解除) / quarantined([(id, 理由)])
          / index_ran / index_error。index が失敗した時だけ marker を残す(次回また試す)。"""
    dirty_dir = _views_dirty_path(rt)
    out = {"recovered": [], "stale_cleared": [], "quarantined": [], "index_ran": False, "index_error": ""}
    if not dirty_dir.exists():
        return out
    markers = sorted(p for p in dirty_dir.iterdir() if p.is_file())
    if not markers:
        return out
    qdir = _views_quarantine_path(rt)

    def _quarantine(marker: Path, reason: str) -> None:
        try:
            old = marker.read_text(encoding="utf-8", errors="replace")
        except OSError:
            old = ""
        _write_marker(qdir / marker.name, f"{reason}\n--- 元のmarker ---\n{old}", required=True)
        _remove_marker(marker)
        out["quarantined"].append((marker.name, reason))

    for marker in markers:
        eid = marker.name
        if not _EVENT_ID_RE.fullmatch(eid):
            _quarantine(marker, f"marker名がevent_idの形式ではない: {eid!r}")
            continue
        try:
            _find_event(rt, eid)
        except RTError as exc:
            if "見つかりません" in str(exc):
                # JSONL追記前にkillされたprecommit marker。正本が無いので安全に除去。
                _remove_marker(marker)
                out["stale_cleared"].append(eid)
            else:
                _quarantine(marker, f"正本の検証に失敗: {exc}")
            continue
        try:
            materialize_markdown(rt, eid, defer_clear=True)
        except Exception as exc:  # RTError も OSError も、1件の失敗で口を閉じない
            _quarantine(marker, f"{exc.__class__.__name__}: {exc}")
            continue
        out["recovered"].append(eid)

    if out["recovered"] and run_index:
        script = index_script or (Path(__file__).resolve().parent / "roundtable_index.py")
        try:
            proc = subprocess.run(
                [sys.executable, str(script)], check=False, env=dict(os.environ, ROUNDTABLE_DIR=str(rt)),
                capture_output=True, text=True, timeout=600)
            if proc.returncode != 0:
                out["index_error"] = (proc.stderr or proc.stdout or "").strip()[-300:] or f"rc={proc.returncode}"
        except Exception as exc:
            out["index_error"] = f"{exc.__class__.__name__}: {exc}"
        out["index_ran"] = not out["index_error"]
        if out["index_error"]:
            return out   # markerは残す。次の回収でまた試す(正本は無事)
        for eid in out["recovered"]:
            clear_projection_marker(rt, eid)
    elif out["recovered"]:
        for eid in out["recovered"]:
            clear_projection_marker(rt, eid)
    return out


def count_quarantined(rt: Path) -> int:
    qdir = _views_quarantine_path(rt)
    return sum(1 for p in qdir.iterdir() if p.is_file()) if qdir.exists() else 0


def clear_projection_marker(rt: Path, event_id: str) -> dict:
    """Markdownと人間向け索引の両方が成功した後だけ呼ぶ。"""
    if not _EVENT_ID_RE.fullmatch(event_id or ""):
        raise RTError(f"event_idの形式が不正です: {event_id!r}")
    dirty_dir = _views_dirty_path(rt)
    _remove_marker(dirty_dir / event_id)
    try:
        dirty_dir.rmdir()
        _fsync_dir(dirty_dir.parent)
    except OSError:
        pass
    return {"event_id": event_id, "projection_dirty": False}


def _post_pipeline_call(rt: Path, *, sender: str, to: str, content: str,
                        type: str, thread_id="", reply_to="", report_path="",
                        topic=None, proof="未検証", terminal_once=False, mado="",
                        dedup_key="") -> dict:
    """管理CLI/workerの書込みもpost pipelineへ一本化する。"""
    post = Path(__file__).resolve().with_name("roundtable_post.sh")
    if not post.is_file():
        raise RTError(f"投稿pipelineがありません: {post}")
    env = dict(os.environ)
    for key in ("RT_REQUEST_ID", "RT_THREAD_ID", "RT_REPLY_TO",
                "RT_REPORT_PATH", "RT_DEDUP_KEY", "RT_TERMINAL_ONCE"):
        env.pop(key, None)
    env["ROUNDTABLE_DIR"] = str(rt)
    env["RT_SOURCE"] = "admin_cli"
    if thread_id:
        env["RT_THREAD_ID"] = thread_id
    if reply_to:
        env["RT_REPLY_TO"] = reply_to
    if report_path:
        env["RT_REPORT_PATH"] = report_path
    if dedup_key:
        env["RT_DEDUP_KEY"] = dedup_key
    if terminal_once:
        env["RT_TERMINAL_ONCE"] = "1"
    # 2026-09-29 Code ai-bd23(札 negai-20260929-135252-486b): claim/progress 等に窓の名の欄が無く、
    #   本物の Code の窓の投稿が2日で54件 mado 空になり、コックピットの本人判定(白名簿)から落ちていた。
    #   渡されなければ呼び手の MADO をそのまま通す(emit の既定と同じ振る舞い)。
    if mado:
        env["MADO"] = mado
    cmd = [str(post), sender, to or "全員", content,
           "" if topic is None else str(topic), type, proof]
    try:
        result = subprocess.run(
            cmd, check=False, env=env, capture_output=True, text=False, timeout=360)
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise RTError(f"投稿pipelineの起動に失敗: {exc}") from exc
    stdout = (result.stdout or b"").decode("utf-8", errors="replace")
    stderr = (result.stderr or b"").decode("utf-8", errors="replace")
    if result.returncode != 0:
        detail = (stderr + stdout).strip()[-800:]
        raise RTError(f"投稿pipelineが失敗(exit={result.returncode}): {detail}")
    match = re.search(r"(?m)^RT_RESULT\s+.*?event_id=(\S+)", stdout)
    if not match:
        raise RTError("投稿pipelineの成功結果にevent_idがありません")
    return _find_event(rt, match.group(1))


def emit_auto_terminal(rt, *, sender, to, content, type, thread_id, reply_to,
                       report_path="") -> dict:
    """自動worker/recovery用のappend-or-get終端CAS。

    希望typeと異なる先行winnerがある場合も、その既存event全体を返す。
    callerは返却typeへreceipt/job/dispatcherを収束させる。
    """
    return _post_pipeline_call(
        Path(rt), sender=sender, to=to, content=content, type=type,
        thread_id=thread_id, reply_to=reply_to, report_path=report_path,
        topic=None, terminal_once=True)


# ---------------- rebuild（専用ロック＋PID固有tmp・Codex P1-1） ----------------

def rebuild_index(rt) -> dict:
    _ensure_dirs(rt)
    with _rebuild_lock(rt).open("w") as rlk:
        fcntl.flock(rlk, fcntl.LOCK_EX)          # rebuild同士を直列化
        with _events_lock(rt).open("w") as elk:
            fcntl.flock(elk, fcntl.LOCK_EX)      # emitとも直列化（整合snapshot）
            try:
                tmp = _index_dir(rt) / f"roundtable.db.{os.getpid()}.tmp"
                if tmp.exists():
                    tmp.unlink()
                con = sqlite3.connect(str(tmp))
                try:
                    con.executescript(_DDL)
                    con.execute("CREATE INDEX IF NOT EXISTS ix_topic ON events(topic)")
                    con.execute("CREATE INDEX IF NOT EXISTS ix_topic_slug ON events(topic_slug)")
                    n = dup = 0
                    canonical_events = list(
                        _iter_events_unlocked(rt, _log=True, _strict=True))
                    _validate_event_sequence(canonical_events)
                    for e in canonical_events:
                        if _db_apply(con, e):
                            n += 1
                        else:
                            raise RTError(
                                f"JSONL正本のevent_id/dedup_keyが競合しています: "
                                f"{e.get('event_id')}")
                    con.commit()
                finally:
                    con.close()
                os.replace(tmp, _db_path(rt))
                for pth in (_db_path(rt), *_events_files(rt), _cursors_file(rt)):
                    try:                       # 既存data も 600 へ移行（Codex cond7）
                        if pth.exists():
                            os.chmod(pth, 0o600)
                    except OSError:
                        pass
                _clear_dirty(rt)              # 整合回復完了→dirty解除（Codex P1-E）
                _fsync_dir(_index_dir(rt))
                nthreads = 0
                c2 = sqlite3.connect(str(_db_path(rt)))
                try:
                    nthreads = c2.execute("SELECT COUNT(*) FROM threads").fetchone()[0]
                finally:
                    c2.close()
                return {"events": n, "threads": nthreads, "dedup_skipped": dup}
            finally:
                fcntl.flock(elk, fcntl.LOCK_UN)


def _ensure_index(rt):
    """索引が無い or dirty（DB書込み失敗の痕跡）なら再構築（emit・read前の保険・Codex P1-E）。"""
    if not _db_path(rt).exists() or _dirty_path(rt).exists():
        rebuild_index(rt)


@contextmanager
def _clean_events_exclusive(rt):
    """dirty確認とevents lock取得のTOCTOUを閉じた書込みゲート。"""
    while True:
        fh = _events_lock(rt).open("w")
        fcntl.flock(fh, fcntl.LOCK_EX)
        # lock取得後に欠損/dirtyを判定する。先に確認すると、
        # lock待ち中に別writerがcrashしたstale DBを使うTOCTOUになる。
        # また通常のwriterが一時markerを置いているだけの間に、
        # 待機writerが不要なrebuildを連発することも防ぐ。
        if not _db_path(rt).exists() or _dirty_path(rt).exists():
            fcntl.flock(fh, fcntl.LOCK_UN)
            fh.close()
            _ensure_index(rt)
            continue
        try:
            yield fh
        finally:
            fcntl.flock(fh, fcntl.LOCK_UN)
            fh.close()
        return


# ---------------- cursor（既読管理・fail-open回復・Codex P1-4/P2-3） ----------------

def _read_cursors(rt) -> dict:
    f = _cursors_file(rt)
    if not f.exists():
        return {}
    try:
        return json.loads(f.read_text(encoding="utf-8"))
    except Exception:
        # 破損は黙って捨てない：警告ログを残す（Codex P2-3）
        _log_corrupt(rt, "cursors.json", 0, "cursor_json_broken", "")
        return {}


def _cursor_of(cursors: dict, participant, ids=None):
    """この本体の読了位置。**古い生の名のキーも見て、先へ進んでいる方を採る。**

    2026-09-26 ai-12ef: cursors.json は昔「名乗った名のまま」をキーにしていた
    （ChatGPT / GPTくくる / くくる が別々の位置を持てた）。書く側は本体名へ揃えたが、
    古いファイルのキーは消さない。読む時に同じ本体のキーを全部見て、正本の並び(ids)で
    一番先の物を採る。並びで決まらない（ids 無し・全部台帳に無い）時は本体名のキーを優先する。
    """
    hon = _canon_name(participant)
    kouho = [(k, v) for k, v in (cursors or {}).items()
             if isinstance(k, str) and v and _canon_name(k) == hon]
    if not kouho:
        return None
    if len(kouho) == 1:
        return kouho[0][1]
    ichi = {eid: i for i, eid in enumerate(ids)} if ids is not None else {}

    def _saki(kv):
        k, v = kv
        return (ichi.get(v, -1), 1 if k == hon else 0)
    return max(kouho, key=_saki)[1]


def _write_cursor(rt, participant, last_event_id):
    _ensure_dirs(rt)
    with _cursors_lock(rt).open("w") as lk:
        fcntl.flock(lk, fcntl.LOCK_EX)
        try:
            cur = _read_cursors(rt)
            # 2026-09-26 ai-12ef: キーは本体名（ChatGPT で進めても くくる の位置が進む）。
            #   古い生の名のキーは消さない（_cursor_of が先へ進んでいる方を採る）。
            cur[_canon_name(participant)] = last_event_id
            f = _cursors_file(rt)
            tmp = f.with_suffix(".json.tmp")
            with tmp.open("w", encoding="utf-8") as out:
                json.dump(cur, out, ensure_ascii=False)
                out.flush()
                os.fsync(out.fileno())          # tmpをfsync（Codex P2-3）
            os.replace(tmp, f)
            try:
                os.chmod(f, 0o600)
            except OSError:
                pass
            _fsync_dir(_index_dir(rt))
        finally:
            fcntl.flock(lk, fcntl.LOCK_UN)


def _meishi_ate(participant, ev) -> bool:
    """**名指しされた宛先か**（全員宛は数えない）。

    ★`_is_recipient` とは**別の問い**だ（2026-09-21・独立検証 ai-d789 の2巡目）。
      `_is_recipient`  … 「この投稿は自分に**届く**か」→ 全員宛は True（読む物を落とさない側）
      `_meishi_ate`    … 「この投稿は自分が**名指しで頼まれた**物か」→ 全員宛は False
      同じ物を指す2つの式ではなく、**答えるべき問いが違う**。関係は必ず
      `_meishi_ate ⊆ _is_recipient` で、差は全員宛の分だけ ―― これは試験で縛ってある
      （scripts/test_juuyou_shinchaku.py T15）。
      どちらを使うか迷ったら: **読ませる物**を選ぶなら _is_recipient、
      **返事を待たせている物**を数えるなら _meishi_ate。


    2026-09-21 ネオ ai-c00c（独立検証 ai-d789 の指摘）:
      `_pending_event_ids` は to を**分割せず丸ごと**名寄せして比べていた。
      その結果 `to="Code, Claude"` の依頼は Code の待ち行列にも Claude の待ち行列にも入らない。
      **同じファイルの `_is_recipient` は分割している**ので、
      「探せば在るのに、誰の『返事待ち』にも出ない」という一番たちの悪い形になっていた。
      数え方を2箇所に書かないため、分割はここ1箇所に置いて両方から呼ぶ。
    """
    p = _canon_name(participant)
    for x in re.split(r"[,、/\s]+", str(ev.get("to") or "")):
        x = x.strip()
        if x and not _wa_broadcast(x) and _canon_name(x) == p:
            return True
    for r in (ev.get("recipients") or []):
        if r not in RT_BROADCAST and (_canon_name(r) == p or r == participant):
            return True
    return False


def _is_recipient(participant, ev) -> bool:
    """この投稿が自分に**届く**か（全員宛も True）。

    ★「自分が名指しで頼まれたか」は別の問い＝`_meishi_ate` を使う。混ぜると、
      全員宛の指示が全員の『返事待ち』に並んで騒がしくなるか、逆に誰の欄にも出なくなる。


    2026-09-20 ネオ ai-38e6: **両側を揃えてから比べる**ようにした。
      前は宛先側だけ `_canon_recipient` を通し、participant は素のまま比べていたので、
      participant="ChatGPT" は to="くくる" を自分宛と見なせなかった（実測で247件の取りこぼし）。
    """
    to = ev.get("to", "")
    if _wa_broadcast(to):
        return True
    p = _canon_name(participant)
    # to は "Code,くくる" のように複数書かれる事がある
    for x in re.split(r"[,、/\s]+", to or ""):
        if x.strip() and _canon_name(x) == p:
            return True
    for r in (ev.get("recipients") or []):
        if r in RT_BROADCAST or _canon_name(r) == p or r == participant:
            return True
    return False


def inbox(rt, participant, *, unread_only=True, limit=50, newest_first=False,
          since="", event_types=None, statuses=None, direct_only=False,
          pending_only=False) -> list:
    """参加者宛のイベントを、最新・期間・種別・状態で絞る。

    既定の読み方（未読・古い→新しい・末尾50件）は維持する。運用検索では
    newest_first/since/event_types/statuses/direct_onlyを明示して大量全件走査を避ける。
    """
    cursors = _read_cursors(rt)
    all_events = list(iter_events(rt))
    ids = [e["event_id"] for e in all_events]
    last = _cursor_of(cursors, participant, ids)
    if unread_only and last is not None:
        if last in ids:
            idx = ids.index(last)
            candidates = all_events[idx + 1:]
        else:
            # cursorが台帳に無い（破損・退避・欠損）→ fail-open＋警告（Codex P1-4）
            _log_corrupt(rt, "cursors.json", 0, f"stale_cursor:{participant}:{last}", "")
            candidates = all_events
    else:
        candidates = all_events
    kinds = set(event_types or [])
    states = set(statuses or [])
    pending_ids = _pending_event_ids(rt, participant, events=all_events) if pending_only else set()
    since_value = str(since or "").strip()
    if since_value:
        try:
            # 日付だけも受理する。比較は正本のISO 8601文字列同士で行う。
            time.strptime(since_value[:10], "%Y-%m-%d")
        except (ValueError, TypeError) as exc:
            raise RTError("--since は YYYY-MM-DD またはISO 8601で指定してください") from exc
    out = []
    for event in candidates:
        # 2026-09-20 ネオ ai-38e6: 差出人も名義を揃える（自分が ChatGPT 名義で出した物を、
        #   くくる名義で見た時に「他人からの未処理」として二重に数えない）。
        if _canon_name(event.get("sender") or "") == _canon_name(participant) \
                or not _is_recipient(participant, event):
            continue
        if pending_only and event.get("event_id") not in pending_ids:
            continue
        if direct_only:
            direct = any(
                recipient not in RT_BROADCAST
                and (_canon_name(recipient) == _canon_name(participant)
                     or recipient == participant)
                for recipient in (event.get("recipients") or [])
            )
            if not direct:
                continue
        if since_value and str(event.get("timestamp") or "") < since_value:
            continue
        if kinds and event.get("type") not in kinds:
            continue
        if states and event.get("status") not in states:
            continue
        out.append(event)
    if newest_first:
        out.reverse()
        return out[:limit] if limit else out
    return out[-limit:] if limit else out


def advance_cursor(rt, participant) -> str | None:
    last_id = None
    for ev in iter_events(rt):
        last_id = ev["event_id"]
    if last_id:
        _write_cursor(rt, participant, last_id)
    return last_id


# ---------------- 生成ビュー（Phase 4） ----------------

def _all_threads(rt) -> dict:
    threads = {}
    for e in iter_events(rt):
        threads.setdefault(e["thread_id"], []).append(e)
    return threads


def _thread_status(evs):
    status = None
    for e in evs:
        s = _TYPE_STATUS.get(e["type"])
        if status is None and s is not None or e["type"] in STATE_CHANGING and s is not None:
            status = s
    return status or "open"


def _thread_claimant(evs):
    c = None
    for e in evs:
        if e["type"] == "claim":
            c = e["sender"]
    return c


def _thread_summary(th, evs) -> dict:
    return {"thread_id": th, "status": _thread_status(evs), "claimant": _thread_claimant(evs),
            "opener": evs[0]["sender"] if evs else None,
            # 2026-08-22: 60→150。60字だと題が途中で切れ、王が何の話題か分からない。
            # 表示側(_subj)が畳むので、元は長めに持つ。
            "subject": (evs[0]["content"] or "")[:150] if evs else "",
            "first_ts": evs[0]["timestamp"] if evs else None,
            "last_ts": evs[-1]["timestamp"] if evs else None, "events": len(evs)}


def _individual_recipient_names(e) -> tuple[tuple[str, ...], bool]:
    """Recipients used only for projection/answer checks; never parse guessed IDs.

    For a comma-joined display, accept the *explicit* canonical event recipients
    when their individual literal names match the display components. Missing
    or inconsistent arrays are unknown, not an instruction to split and infer.
    The single-recipient legacy case preserves its existing scalar fallback.
    """
    raw = (e.get("to") or "").strip()
    if not raw:
        return (), False
    declared = e.get("recipients")
    parts = [name.strip() for name in raw.split(",")]
    composite = len(parts) > 1 or (type(declared) is list and len(declared) > 1)
    if composite:
        if (type(declared) is not list or len(declared) != len(parts)
                or len(parts) < 2 or any(not x or "," in x for x in parts)
                or any(type(x) is not str or not x.strip() or x.strip() != p
                       for x, p in zip(declared, parts))):
            return (), False
        return tuple(_canon_name(part) for part in parts), True
    if type(declared) is list:
        if len(declared) != 1 or type(declared[0]) is not str:
            return (), False
        if _canon_name(declared[0]) != _canon_name(raw):
            return (), False
    elif declared is not None:
        return (), False
    return (_canon_name(raw),), True


def _unknown_recipient_in_projection(e, last_seen: dict) -> bool:
    """Unknown means absent/invalid canonical recipients or an actual unseen member.

    Display commas are not a principal. One missing member still merits the
    old boolean warning, but existing answers and work states stay untouched.
    """
    members, valid = _individual_recipient_names(e)
    return not valid or any(name not in last_seen for name in members)


def _pending_questions(evs, resolved_qids=None) -> list:
    """このスレッドで**まだ本人が答えていない質問**を返す。

    ⚠️ 「同スレッドに answer が在る」だけで待ちから外してはいけない。
       2026-08-01 実測: Code→Claude の質問(10:51:00)は、65秒後に
       `source=chimu_bell` のベル代理回答で消え、ダイジェストは
       「返答待ち0件」と表示した。**王が口で伝令する羽目になった。**

    数える条件（全部満たして初めて「答えた」）:
      1. type=answer
      2. **宛先本人が**書いた（sender == 質問の宛先）
      3. ベル代理でない（source != chimu_bell）… 人格の名を騙った物は返事に数えない
      4. 質問より後に来た

    2026-08-02: 解決台帳 resolutions.jsonl に登録された質問event_idは
       「回答が別スレッドに紐付いたが本人回答は確認済み」として除外する。
       王命「古い札の掛け替え」の実装。
    """
    resolved_qids = resolved_qids or set()
    out = []
    for index, e in enumerate(evs):
        # 2026-08-01: proposal も「本人の返事を待っている物」として同じ扱いにする。
        if e.get("type") not in ("question", "proposal"):
            continue
        if e.get("event_id") in resolved_qids:
            continue
        askee = (e.get("to") or "").strip()
        if not askee:
            continue
        # 2026-09-26 ai-12ef: 比べるのは本体名（GPTくくる宛の質問に くくる 名義で答えても済になる）。
        askee_hon = _canon_name(askee)
        # timestampは分単位なので同じ分の前後を判定できない。JSONL正本の追記順を使う。
        # 2026-09-12 くくる監査①: reply_to を見ずに「宛先本人の後続answer」だけで済扱いすると、
        #   同スレッドに同じ相手向けの question が複数在る時、1件の回答で全部消える。
        #   実データで13スレッドが該当した。方針:
        #     - answer.reply_to があれば → その質問だけを済扱い
        #     - reply_to 無しは、そのスレッドに未解決の同askee向け質問が「1件だけ」の時のみ済扱い
        #       (曖昧なら安全側に倒して待ちを残す・王が急かされる方向)
        qid = str(e.get("event_id") or "")
        answered = False
        for a in evs[index + 1:]:
            if a.get("type") != "answer":
                continue
            if _canon_name(a.get("sender") or "") != askee_hon:
                continue
            if a.get("source") == "chimu_bell":
                continue
            rt = str(a.get("reply_to") or "")
            if rt:
                if rt == qid:
                    answered = True
                    break
                # 別の質問への答え。この質問は消さない。
                continue
            # reply_to 無し: 未解決の同askee向け question 総数が1件なら済とみなす
            unresolved_same_askee = 0
            for other in evs:
                if other.get("type") not in ("question", "proposal"):
                    continue
                if _canon_name((other.get("to") or "").strip()) != askee_hon:
                    continue
                if other.get("event_id") in resolved_qids:
                    continue
                unresolved_same_askee += 1
            if unresolved_same_askee <= 1:
                answered = True
                break
            # 複数曖昧 → この answer では確定させない
        if not answered:
            out.append(e)
    return out


def _honnin_betsu_ito_kaitou(threads) -> set[str]:
    """本人が**別の糸**で reply_to を付けて答えた質問の event_id。

    2026-09-05 修理(バグ探し高#2の後半): 元糸が完了済みだと本人は新糸へ
    note+reply_to で答える運用(実測: Claude 09-04 08:36 が Code→Claude 08-27 06:29 へ
    そうした)。共有頭脳の session_start はこれを答えと数えるのに、digest の
    _pending_questions は同じ糸の answer しか見ず、9日「待ち」と嘘を出していた。
    口が2つあって正反対を言う状態をやめる。数える条件は _pending_questions と同じ芯:
      1. reply_to が質問(question/proposal)を指す
      2. **宛先本人**が書いた(sender == 質問の to)
      3. ベル代理でない(source != chimu_bell)。運ぶ役(ベル・チム)の受領noteは答えではない
      4. 種別が answer/note/complete/blocked/proposal
    """
    askee_of: dict[str, str] = {}
    for evs in threads.values():
        for e in evs:
            if e.get("type") in ("question", "proposal"):
                askee_of[str(e.get("event_id") or "")] = (e.get("to") or "").strip()
    HAKOBI_YAKU = {"ベル", "チム"}
    out: set[str] = set()
    for evs in threads.values():
        for e in evs:
            rt_id = str(e.get("reply_to") or "")
            if not rt_id or rt_id not in askee_of:
                continue
            who = (e.get("sender") or "").strip()
            # 2026-09-26 ai-12ef: 本人かどうかは本体名で見る（ChatGPT 名義の答え＝くくるの答え）。
            if not who or _canon_name(who) != _canon_name(askee_of[rt_id]) or who in HAKOBI_YAKU:
                continue
            if e.get("source") == "chimu_bell":
                continue
            if e.get("type") in ("answer", "note", "complete", "blocked", "proposal"):
                out.add(rt_id)
    return out


def pending_question_events(events, resolved_qids=None):
    """Shared read-only question projection using the digest's existing policy.

    Preserve canonical append order within each thread. Session-start callers
    may filter proposals or participants after applying the shared policy.
    """
    threads = {}
    for event in events:
        key = event.get("thread_id") or event.get("event_id")
        threads.setdefault(key, []).append(event)
    resolved = set(resolved_qids or ()) | _honnin_betsu_ito_kaitou(threads)
    return [question for rows in threads.values()
            for question in _pending_questions(rows, resolved)]


def _resolved_qids(rt, threads=None) -> set[str]:
    """待ち行列から下ろしてよい質問ID = 解決台帳 ∪ 本人の別糸回答。

    2026-09-07 ネオ(くくる依頼1の途中で発見): 呼び手が糸ごとにこれを呼び直し、その度に正本を全部
    読んでいた(view unclaimed で 164回×2624件=43万件の解析・投稿1件が14.5秒、うち13秒が索引)。
    読み終えた threads を渡せる形にし、呼び手は1回だけ計算して使い回す。判定の中身は変えていない。"""
    if threads is None:
        threads = _all_threads(rt)
    return set(_load_resolutions(rt).keys()) | _honnin_betsu_ito_kaitou(threads)


def _ito_ga_owatte_iru(evs, taisho, claimant=None, aite="") -> str:
    """この糸の complete が、対象(質問/指示)を**終わらせている**か。終わらせていれば理由の1文を返す。

    ★2026-09-23 ネオ ai-01bb（王 2026-09-23「Codeに送って区別できるようにすればいいよ」を
      Claude経由で受領・円卓 event 01790124974520247301-6af15518・札 negai-20260923-100404-7f25）

    ■なぜ要るか（実測 2026-09-23 10:10）
      見張りの「答え待ち 27件」のうち **20件(74%)**、「取り手なし 17糸」のうち **10糸(59%)** が
      **糸が既に complete** だった。同じ「終端なら出さない」判定を受信箱側
      (_pending_event_ids の 2026-08-29 のコメント) は持っているのに、
      **見張りが使う view() にだけ入っていなかった。**
      このファイルの 2026-09-21 の教訓「片方だけ直すと必ず腐る」が、そのまま起きていた。

    ■⚠️なぜ「糸が complete なら除く」と単純に書かないか
      2026-08-02 にその形を**意図的に外してある**（下の waiting_reply / unclaimed のコメント）。
      実害 = 話題「全AI共通の掟」で 05:19 の**無関係な自動wake完了1件**が、
      02:17(Codex宛)と02:26(Claude宛)の未回答質問2件を消していた。
      世界にも両方の型がある（detent #2793 = 親が閉じたのに質問が Needs-you に残る /
      逆に「親が閉じても本当に残る質問がある」）。だから**2つとも満たす時だけ**終わりとみなす:

        ① complete が対象より **後** に打たれている
        ② complete を打ったのが **その糸の担当者(claim した人) か 対象の宛先**
           （無関係な第三者の完了では消さない ＝ 2026-08-02 の犯人はこれ）

      実測 2026-09-23: 該当30件は **30件とも①②を満たし、第三者による完了は0件**だった。

    ■黙って消さない
      ここは真偽ではなく**理由の1文**を返す。呼び手は行を捨てずに owari_zumi の印として持ち、
      見張りが警報から外して**別欄で数える**。truthboard の
      「理由は間違ってよいが、黙って間違ってはいけない」に合わせた。
    """
    ts = (taisho.get("timestamp") or "") if isinstance(taisho, dict) else str(taisho or "")
    tantou = (claimant or "").strip()
    atesaki = (aite or "").strip()
    # ⚠️2026-09-23 同日に自分で塞いだ穴: **timestamp は分単位**なので、`cts > ts` だけで
    #   見ると**同じ分に打たれた完了を取りこぼす**。実測で1件あった ――
    #   糸 01789451762132942709: 15:07 の指示と 15:07 の complete(Codex) が同分で、
    #   指示だけが「取り手なし」に残り続けていた。
    #   このファイルの _last_response が同じ問題を既に解いている
    #   （「timestampは分単位。同分内の質問→応答を落とさないよう正本の追記順で後続だけを見る」）。
    #   **同じ手に揃える**: 対象の位置を正本の並びで取り、**その後ろに現れる完了**を見る。
    #   時刻は「対象より前でない」だけを条件にする（前の完了は関係ないまま）。
    taisho_id = (taisho.get("event_id") or "") if isinstance(taisho, dict) else ""
    hajime = 0
    if taisho_id:
        for i, e in enumerate(evs):
            if e.get("event_id") == taisho_id:
                hajime = i + 1
                break
    for e in evs[hajime:]:
        if e.get("type") != "complete":
            continue
        cts = e.get("timestamp") or ""
        if cts < ts:
            continue                       # ①対象より前の完了は、この対象を終わらせていない
        who = (e.get("sender") or "").strip()
        # 2026-09-26 ai-12ef: 担当・宛先と同じ**本体**が閉じたか（表示は生の名のまま）。
        if who and ((tantou and _canon_name(who) == _canon_name(tantou))
                    or (atesaki and _canon_name(who) == _canon_name(atesaki))):
            return f"{who} が {cts[:16]} に糸を complete で閉じた"
    return ""


def _pending_event_ids(rt, participant: str, zenin: bool = False, *, events=None) -> set[str]:
    """本人が今処理すべき未回答質問・未claim指示のevent ID。

    zenin=True で**全員宛も入れる**（2026-09-21 ネオ ai-c00c・独立検証 ai-d789 の指摘）。
      「全員へ。これを至急やってくれ」という未claimの指示は、**誰の返事待ちにも出ていなかった**。
      既定を変えると全AIの待ち行列が一斉に増えるので、**呼ぶ側が選ぶ**形にした
      （本番の実測では該当6件。溢れはしないが、数え方を勝手に変えない）。
    """
    pending: set[str] = set()
    if events is None:
        threads = _all_threads(rt)
    else:
        # Already validated by iter_events under the shared source lock.
        threads = {}
        for event in events:
            threads.setdefault(event["thread_id"], []).append(event)
    resolved_qids = _resolved_qids(rt, threads)
    for evs in threads.values():
        for question in _pending_questions(evs, resolved_qids):
            # 2026-09-20 ネオ ai-38e6: 名義を揃えてから比べる（くくる=ChatGPT=GPTくくる）。
            #   素の完全一致だと、くくるが ChatGPT 名義で見た時に自分宛の質問が消える。
            # 2026-09-21 ネオ ai-c00c: 丸ごと比べると to="Code, Claude" が誰にも届かない。
            if _meishi_ate(participant, question) or (zenin and _wa_broadcast(str(question.get("to") or ""))):
                pending.add(str(question.get("event_id") or ""))
        # complete thread は終端であり、emit 側でも状態変更を拒否する。
        # そこに残った未claim instructionをpendingへ出し続けると、
        # 正規のclaim手段が無く「幽霊pending」になるため、同じ終端判定を
        # pending算出にも適用する。
        thread_complete = any(event.get("type") == "complete" for event in evs)
        instructions = [
            event for event in evs
            if event.get("type") == "instruction" and not thread_complete
        ]
        claims = [event for event in evs if event.get("type") == "claim"]
        handled = {
            str(event.get("reply_to") or "") for event in claims if event.get("reply_to")
        }
        # 2026-10-09 Codex: instructionへの明示replyも処理済みと数える。
        # claimだけを見ると、answer/blockedで正式に返した依頼が永久にpendingへ残る。
        # ベル代理は本人回答ではないため、質問側と同じく除外する。
        responses = [
            event for event in evs
            if event.get("type") in ("answer", "blocked")
            and event.get("source") != "chimu_bell"
        ]
        handled.update(
            str(event.get("reply_to") or "")
            for event in responses
            if event.get("reply_to")
        )
        if len(instructions) == 1 and any(not event.get("reply_to") for event in claims):
            handled.add(str(instructions[0].get("event_id") or ""))
        for instruction in instructions:
            if instruction.get("event_id") in handled:
                continue
            # 2026-08-29: 完了済みthreadに残ったinstructionは **構造上永久に消せなかった**。
            #   claimを付けようにも emit の終端ガードが完了済みthreadへの投稿を拒否するため。
            #   実測: Code宛pending 19件のうち8件(42%)がこの幽霊で、
            #   毎日届く本物の依頼(音声台本)を埋もれさせていた。
            #   → 解決台帳(resolutions.jsonl)に登録された物は、質問と同じく下ろす。
            #   台帳は追記のみ・削除不可なので、消した記録が必ず残る。
            if str(instruction.get("event_id") or "") in resolved_qids:
                continue
            # 2026-09-21 ネオ ai-c00c: 質問側と同じ数え方に揃える（片方だけ直すと必ず腐る）。
            if _meishi_ate(participant, instruction) or (zenin and _wa_broadcast(str(instruction.get("to") or ""))):
                pending.add(str(instruction.get("event_id") or ""))
    pending.discard("")
    return pending


def _elapsed(ts: str) -> str:
    """経過時間。15時間放置が一目で分かるように必ず出す。"""
    try:
        from datetime import datetime
        t = datetime.fromisoformat(ts)
        sec = int((datetime.now(t.tzinfo) - t).total_seconds())
        sec = max(sec, 0)
        if sec >= 48 * 3600:            # 2026-08-22: 3桁の時間(487h58m)は直感で読めない
            return "%d日" % (sec // 86400)
        return "%dh%02dm" % (sec // 3600, (sec % 3600) // 60)
    except Exception:
        return "?"


def _to_king(e) -> bool:
    """**明示的に王が宛先か。** 「全員」は含めない。

    ⚠️ `_is_recipient("王", e)` は "全員" を真にするので、ここでは使えない。
       2026-08-01 実測: 全員宛の完了報告(17:00)が「王の返答待ち」に立った。
    """
    to = (e.get("to") or "")
    if "王" in to.replace("全員", "").replace("みんな", ""):
        return True
    return any(r == "王" for r in (e.get("recipients") or []))


def _to_someone(e) -> bool:
    """特定の相手宛か（全員宛は誰の宿題でもないので除く）。

    2026-09-20 ネオ ai-38e6: **"all" を全員宛として拾うようにした。**
      RT_BROADCAST には最初から "all" が在ったのに、ここが「全員/みんな」しか見ておらず、
      実測で **to="all" の44件が『特定の相手宛』に化けていた**（＝誰かの宿題として立つ）。
    """
    to = (e.get("to") or "").strip()
    if not to or _wa_broadcast(to):
        return False
    return True


def _last_response_ts(evs, question_ev) -> str:
    """王命2026-08-22「digestの待ち時間を最終応答起点へ変える」の心臓部。

    同スレッドで質問より後に来たイベントのうち、**投稿者が質問者本人でない**
    最新のタイムスタンプを返す。無ければ空文字。

    ▼ 線引き（Codeが決めた・王委任「Code側で決めてよい」2026-08-22）
      応答に含める:
        ・受領連絡（type=note・短くても中身が薄くても）
          → 「取り下げるべきか」の判断には、誰かが札に触れた事実自体が要る
        ・脇からのnote（第三者が「これ動いてるよ」的な追記）
          → スレッドが生きている証拠
        ・claim / proposal / answer
      応答に含めない:
        ・質問者(asker)本人の再投稿（催促は応答ではない）
          → 質問者が独り言を書いても時計は戻さない

    ▼ 反例（現時点で汲めない物・敢えて汲まない）
      ・別スレッドに落ちた受領連絡は拾わない（reply_toも無いことが多く、
        cross-thread検索は "また別の数字の嘘" を生みやすい）
      ・resolutions.jsonl の掛け替えは _pending_questions が既に見ている
    """
    return _last_response(evs, question_ev)[0]


def _last_response(evs, question_ev):
    """最終応答の (時刻, 応答した人) を返す。

    2026-08-22 バグ探しで判明: 時刻だけ返していたため、digestが
    「Code → くくる【質問】… 最終応答 08-22 15:56」と出していた。
    **くくるは答えていない**。同スレッドに第三者が書いただけ。
    王には「くくるが返事した」としか読めない ―― 数字が嘘をついていた。
    誰が応答したかを一緒に返し、表示側で宛先本人かどうかを言い分ける。
    """
    asker = question_ev.get("sender") or ""
    # 2026-09-26 ai-12ef: 「質問者本人の再投稿は応答ではない」を本体で見る
    #   （くくる名義で聞き ChatGPT 名義で催促した物を、他人の応答と数えない）。
    asker_hon = _canon_name(asker)
    latest, dare = "", ""
    question_index = next(
        (index for index, event in enumerate(evs)
         if event.get("event_id") == question_ev.get("event_id")),
        -1,
    )
    # timestampは分単位。同分内の質問→応答を落とさないよう正本の追記順で後続だけを見る。
    for x in evs[question_index + 1:] if question_index >= 0 else []:
        xts = x.get("timestamp") or ""
        if _canon_name(x.get("sender") or "") == asker_hon:
            continue
        latest, dare = xts, (x.get("sender") or "?")
    return latest, dare


def _last_seen_by(threads) -> dict:
    """参加者ごとの「円卓全体での最終発言時刻」。

    2026-08-22の実害の直し。くくるは08-21 17:48に「絵の窯の設計依頼を読みました」と
    返したが、その投稿が話題『雑談』へ落ちたため同スレッドには現れず、digestは
    『経過6日』と出し続けた。私(ネオ)はその数字だけを見て「6日放置」と引き継ぎ書に
    書き、王には取り下げの検討まで出た。**実際は受領から22時間だった。**

    ここでは中身を読まない。「その人が質問の後に円卓のどこかで口を開いたか」だけを見る。
    答えたとは言わない。沈黙ではない、とだけ言う（嘘を作らないための線引き）。
    """
    # 2026-09-26 ai-12ef: キーは**本体名**。実害: くくるが 15:47 に ChatGPT 名義で発言しても、
    #   「くくる」のキーは 05:00 のままで「くくるは05:00から沈黙」と出ていた。
    #   引く側も _canon_name を通してから引く（kuchi.get(_canon_name(askee))）。
    seen = {}
    for evs in threads.values():
        for x in evs:
            who = _canon_name((x.get("sender") or "").strip())
            ts = x.get("timestamp") or ""
            if who and ts > seen.get(who, ""):
                seen[who] = ts
    return seen


def view(rt, kind, participant=None, threads=None) -> list:
    # threads: 同じ瞬間の正本を何種類も見る呼び手(roundtable_index の3種)が、
    #   _all_threads を1回だけ作って渡すための口。渡さなければ今までどおり読む。
    #   2026-10-03 ネオ ai-73b1(王命_20261003-1321): 索引1回で正本1.5万件を3回読み3.3秒かかっていた。
    if kind == "unread":
        if not participant:
            raise RTError("view unread には --participant が必要です")
        return inbox(rt, participant, unread_only=True, limit=0)
    if threads is None:
        threads = _all_threads(rt)
    resolved_qids = _resolved_qids(rt, threads)   # 1回だけ(2026-09-07: 糸ごとに呼び直して13秒食っていた)
    kuchi = _last_seen_by(threads)   # 別スレッドに落ちた返事を拾うため（中身は読まない）
    rows = []
    for th, evs in threads.items():
        st = _thread_status(evs)
        if kind == "in_progress" and st in ("claimed", "waiting", "blocked"):
            rows.append(_thread_summary(th, evs))
        elif kind == "waiting_king":
            q = [e for e in _pending_questions(evs, resolved_qids) if _to_king(e)]
            # 2026-08-02: `and st != "complete"` を外した。話題の中の**別件**が1つ
            #   完了しただけで、王への未回答質問が消えていた。答えたかどうかは
            #   _pending_questions が1件ずつ正しく見ている。話題全体で上書きしない。
            # 2026-09-05 修理(バグ探し高#6): q[0] だけ立てていたので、同じ糸に王宛の
            #   未回答質問が2件あると2件目が一枚から消えていた(実測: 09-04 22:34 の
            #   3090候補9件の質問が消え、件数も5件を4件と出した)。waiting_reply 側は
            #   2026-08-01 に同じ穴を直してあった(下の break の顛末)。1問1行にする。
            for qe in q:
                r = _thread_summary(th, evs)
                last_ts, last_by = _last_response(evs, qe)
                r.update(asker=qe["sender"], askee="王",
                         asked_ts=qe.get("timestamp") or "",
                         last_resp_ts=last_ts,
                         last_resp_by=last_by,
                         response_state=("untyped_response" if last_by == "王"
                                         else "third_party_activity" if last_by
                                         else "no_response"),
                         question=(qe.get("content") or "")[:150],
                         # 2026-09-06 ネオ: 同じ台が毎朝立てる質問を1行に畳むために要る。
                         #   畳むキー = 差出人 + 話題。行に無いと畳めない。
                         topic=(qe.get("topic") or ""))
                rows.append(r)
        elif kind == "waiting_reply":
            # 宛先が誰であれ、返事を待っている物を全部立てる（欠陥A本体）。
            # 王宛は waiting_king が持つのでここでは除く。
            for e in _pending_questions(evs, resolved_qids):
                # 王宛は waiting_king が持つ。全員宛は誰の宿題でもないので立てない。
                # 2026-08-02: `or st == "complete"` を外した。実測: 話題
                #   「全AI共通の掟」で、05:19の無関係な自動wake完了1件が
                #   02:17(Codex宛)と02:26(Claude宛)の未回答質問2件を消していた。
                if _to_king(e) or not _to_someone(e):
                    continue
                r = _thread_summary(th, evs)
                askee = (e.get("to") or "?").strip()
                # 2026-09-26 ai-12ef: askee の**表示**は記録の生の文字列のまま。
                #   比べる・引く時だけ本体名（ChatGPT 名義の発言＝くくるの返事）。
                members, valid = _individual_recipient_names(e)
                multi = len(members) > 1
                last_ts, last_by = _last_response(evs, e)
                r.update(asker=e["sender"], askee=askee,
                         asked_ts=e.get("timestamp") or "",
                         last_resp_ts=last_ts,
                         last_resp_by=last_by,
                         response_state=("recipient_activity_partial"
                                         if last_by and valid and multi
                                            and _canon_name(last_by) in members
                                         else "untyped_response"
                                         if last_by and valid and len(members) == 1
                                            and _canon_name(last_by) == members[0]
                                         else "third_party_activity" if last_by
                                         else "no_response"),
                         askee_last_ts=(max((kuchi.get(name, "") for name in members),
                                            default="") if valid else ""),
                         question=(e.get("content") or "")[:150])
                # A combined label is not a participant. Inspect individuals
                # from the exact canonical recipients[] instead. A missing or
                # contradictory array remains unknown, not a guessed split.
                r["recipient_identity_valid"] = valid
                r["recipient_alias_ambiguous"] = bool(
                    multi and len(set(members)) != len(members))
                r["recipient_activity"] = (
                    [{"name": name, "last_seen": kuchi.get(name, "")}
                     for name in dict.fromkeys(members)] if valid else [])
                # 2026-09-23 ネオ ai-01bb: 担当者が**後から**閉じた糸の質問に印を付ける。
                #   行は捨てない（見張りが警報から外して別欄で数える）。判定の理由は
                #   _ito_ga_owatte_iru の説明に全部書いてある。**単純な「糸がcomplete」ではない。**
                r["owari_zumi"] = _ito_ga_owatte_iru(evs, e, _thread_claimant(evs), askee)
                # 2026-09-23 ネオ ai-01bb: **宛先が円卓で一度も口を開いていない**物に印。
                #   実測 09-23: 残った「忘れ」15件のうち4件が、存在しない相手宛だった
                #   （ニライBrain 2件・閉じた窓 ai-25c4/ai-f559系 1件・ai-a071 1件）。
                #   これは「忘れた」のではなく**届く先が無い**。待ち続けても永久に減らない。
                #   ⚠️警報から**外さない**。外すと本当に届けたい物が黙って消える。
                #   名前を分けて「宛先を直す必要がある物」として見せるだけにする。
                r["todokanai"] = bool(askee) and _unknown_recipient_in_projection(e, kuchi)
                rows.append(r)
                # 2026-08-01: ここに break があり、1スレッドに未回答の質問が複数あっても
                # 最初の1件しか立たなかった。実測: thread b469da81 は
                # _pending_questions が2件返すのに、ダイジェストは1件しか出さず、
                # 15:49のClaude宛の質問が誰にも見えないまま埋もれた。
                # このブロックのコメント自身が「返事を待っている物を全部立てる」と
                # 言っている(欠陥A本体)。実装を意図に合わせる。
        elif kind == "unclaimed":
            # 2026-08-02: 以前は「この話題に claim が1件でもあれば着手済み」だった。
            #   claim には reply_to が入っていて**どの指示への着手か**が分かるのに、
            #   突き合わせていなかった。実測: 話題「全AI共通の掟」で、王の自動wake3件への
            #   claim が、Codex→Code の未着手の指示5件(03:53/04:01/04:03/04:10/04:21)まで
            #   着手済みに見せていた。話題全体の complete で消すのもやめる。
            _instructions = [e for e in evs if e["type"] == "instruction"]
            _claims = [e for e in evs if e["type"] == "claim"]
            _claimed = {(e.get("reply_to") or "") for e in _claims
                        if e.get("reply_to")}
            if len(_instructions) == 1 and any(not e.get("reply_to") for e in _claims):
                _claimed.add(_instructions[0]["event_id"])
            # 2026-08-29: **今朝の修理の波及漏れを塞いだ。**
            #   同じ「指示が片付いたか」の判定が受信箱(_pending_event_ids)とここの
            #   2箇所にあり、今朝は受信箱にだけ「解決台帳に載った物は下ろす」を入れた。
            #   結果、12件を台帳へ下ろしても**王の一枚は「未着手20件」のまま**だった。
            #   出口ごとに書くと必ずこうなる(王国の掟9)。同じ台帳をここでも見る。
            _kaiketsu = resolved_qids   # 2026-09-07: ここで糸ごとに _resolved_qids(rt) を呼び直していた(=正本を164回読む)
            _open_instr = [e for e in evs
                           if e["type"] == "instruction"
                           and e.get("event_id") not in _claimed
                           and str(e.get("event_id") or "") not in _kaiketsu]
            if _open_instr:
                # 2026-09-23 ネオ ai-01bb: 担当者が**後から**閉じた指示を分ける。
                #   実測 09-23: 取り手なし17糸のうち10糸が「もう閉じた糸」だった。
                #   糸ごとではなく**指示1件ごと**に見る（1つの糸に生きた指示と
                #   終わった指示が混ざるため。糸で丸めると、どちらかが必ず嘘になる）。
                _tantou = _thread_claimant(evs)
                _owari = {}
                for _e in _open_instr:
                    _riyuu = _ito_ga_owatte_iru(evs, _e, _tantou,
                                                (_e.get("to") or "").strip())
                    if _riyuu:
                        _owari[str(_e.get("event_id") or "")] = _riyuu
                _ikiteru = [_e for _e in _open_instr
                            if str(_e.get("event_id") or "") not in _owari]
                r = _thread_summary(th, evs)
                # unclaimed_count は**生きている指示だけ**を数える（見張りの警報の母数）。
                r["unclaimed_count"] = len(_ikiteru)
                r["oldest_unclaimed"] = (_ikiteru[0].get("timestamp") or "") if _ikiteru \
                    else (_open_instr[0].get("timestamp") or "")
                # 黙って消さない: 何件をどの理由で外したかを行に残す。
                r["owari_zumi_count"] = len(_owari)
                r["owari_zumi"] = next(iter(_owari.values()), "")
                # 2026-09-23 ネオ ai-01bb: 宛先が円卓で一度も口を開いていない指示を数える
                #   （上の waiting_reply 側の説明を読め。警報からは外さない）。
                r["todokanai_count"] = sum(
                    1 for _e in _ikiteru
                    if (_e.get("to") or "").strip()
                    and _unknown_recipient_in_projection(_e, kuchi))
                rows.append(r)
        elif kind == "complete" and st == "complete":
            rows.append(_thread_summary(th, evs))
    # 2026-09-05 修理(バグ探し中#9): 質問の一覧は「放置が長い順」で並べる。
    #   従来は全kind共通で last_ts(話題の最終更新)順だったため、王宛に起票9日の
    #   質問が、別件の投稿で時計が動いた話題の下に沈んでいた(実測の一枚で9日放置が
    #   2日前の質問より下)。放置の長さを決めるのは起票時刻であって話題の更新ではない。
    if kind in ("waiting_king", "waiting_reply"):
        rows.sort(key=lambda r: r.get("asked_ts") or r.get("last_ts") or "")
    else:
        rows.sort(key=lambda r: r.get("last_ts") or "")
    return rows


def thread_history(rt, thread_id) -> list:
    return [e for e in iter_events(rt) if e["thread_id"] == thread_id]


def _rireki_no_ijou(rt, jikan: int = 24):
    """直近 jikan 時間に degraded だった観測の回数と、最後の時刻を返す（2026-09-15 ネオ ai-31d6）。

    ★なぜ要るか（別窓 ai-b83d の独立検証で分かった・私の判断が実測で否定された）:
      「◆共有ループ状態」は loop_state の**今この瞬間**しか持たないので、
      **状態が回復した瞬間に異常が消える**。実際この日、00:20 と 04:20 に
      degraded（外の道 claude.ai→頭脳の口が HTTP 502）が出ているのに、
      09:12 の digest は「healthy / 異常:なし」で**痕跡が1文字も無かった**。
      履歴 roundtable/loop/history.jsonl には残っていたが、**読む口がどこにも無かった**
      ＝残っていても誰も見ない（呼ばれない門番と同じ型）。

    ⚠️履歴は**62MB・10,902行**（1行5.7KB）ある。**全部読むな。**末尾2MBだけ読む
      （実測347行 ≒ 29時間分。24時間は収まる）。読めた範囲での数だと分かるように、
      表示は「直近24時間に**観測が**N回」と書く（状態変化の回数ではない。
      実測2026-09-15: 履歴の観測は5回・円卓への状態変化の投稿は2回。どちらも正しい）。
    """
    p = Path(rt) / "loop" / "history.jsonl"
    try:
        size = p.stat().st_size
        with p.open("rb") as f:
            f.seek(max(0, size - 2_000_000))
            nama = f.read().decode("utf-8", "replace")
    except OSError:
        return 0, ""                      # 履歴が無くても digest は止めない(fail-open)
    from datetime import datetime as _d
    from datetime import timedelta as _td
    kazu, saigo = 0, ""
    for gyou in nama.splitlines()[1:]:    # 先頭行は途中で切れているので捨てる
        if '"state": "degraded"' not in gyou and '"state":"degraded"' not in gyou:
            continue
        m = re.search(r'"observed_at":\s*"([^"]+)"', gyou)
        if not m:
            continue
        try:
            t = _d.fromisoformat(m.group(1))
        except (ValueError, TypeError):
            continue
        if (_d.now(t.tzinfo) - t) <= _td(hours=jikan):
            kazu += 1
            saigo = m.group(1)
    return kazu, saigo


def digest(rt) -> str:
    _shared_threads = _all_threads(rt)
    wk, uc, ip, cp = (view(rt, "waiting_king", threads=_shared_threads),
                      view(rt, "unclaimed", threads=_shared_threads),
                      view(rt, "in_progress", threads=_shared_threads),
                      view(rt, "complete", threads=_shared_threads))
    wr = view(rt, "waiting_reply", threads=_shared_threads)
    resolved = _load_resolutions(rt)

    def _subj(r):
        """題は1行に畳む。2026-08-08: 改行入りの題がそのまま出て、王の一枚で
        1件が2行3行に折れて読めなくなっていた（_w だけが畳んでいた）。"""
        return " ".join((r.get("subject") or "").split())[:70]

    def _l(r):
        who = r.get("claimant") or r.get("opener") or "?"
        return f"  ・[{r['status']}] {_subj(r)}（担当:{who} / 更新:{(r.get('last_ts') or '')[:16]} / {r['events']}件）"

    def _tomatta_gyou(r):
        """止まっている進行中の1行（2026-09-15 ネオ ai-31d6）。

        _l と同じ材料だが、主数字を**放置の長さ**にして、鍵(thread_idの末尾8桁)を添える。
        題は削らない ―― 削ると何の話か分からなくなり、畳んだのではなく消したのと同じになる。
        鍵があれば roundtable_tadoru(鍵) で1件を開ける＝畳んでも辿れる。

        ★「誰かの返事待ちか、閉じ忘れか」を必ず書く（2026-09-15 09時・**私自身の誤読から**）:
          糸の**題は最初の発言のまま**なので、「【王の裁定待ち】…裁可をくれ」という題が
          残っていても、その質問は台帳で既に解決済みの事がある。実測すると
          **止まっている19件は全部「未回答質問0件」**＝誰の返事も待っていなかった。
          それを知らずに私は王へ「王の裁可待ちが埋もれている」と**誤って報告した**。
          行が自分で語らないと、読む側（AIも王も）が題から誤読する。

          世界も同じ所で転んでいて、名前が付いている ――
          **stale blocker text が「あなたの判断待ち」を水増しする**
          (shakacode/agent-coordination-dashboard#112「Classify dead lanes as stuck, not blocked」)。
          直し方も同じ: **「待ち」と「止まっているだけ」を言い分ける。**
          判定は既にこの関数の外で取ってある wk/wr（＝未回答質問を持つ糸）を使うだけで、
          **新しい物差しは1つも作っていない**。
        """
        who = r.get("claimant") or r.get("opener") or "?"
        kagi = str(r.get("thread_id") or "")[-8:]
        machi = ("誰かの返事待ち" if r.get("thread_id") in machi_no_ito
                 else "返事待ちではない＝閉じ忘れ")
        return (f"  ・[{r['status']}] {_subj(r)}（担当:{who} / 放置{_elapsed(r.get('last_ts') or '')}"
                f" / 最終:{(r.get('last_ts') or '')[:16]} / {machi} / 鍵:{kagi}）")

    def _u(r):
        """担当未決の1行。

        2026-08-08: ここは _l() を使い回していたため、行頭に**話題全体の状態**が出て
        `[complete]` と表示されていた（実測9件中4件）。王には「完了した物がなぜ
        未決に並ぶのか」としか読めない。判定自体は正しく、意味は
        「この話題の中に、まだ誰も着手していない指示が n 件ある」。
        行は unclaimed_count / oldest_unclaimed を既に持っているので、そう書く。
        """
        who = r.get("claimant") or r.get("opener") or "?"
        n = r.get("unclaimed_count", 0)
        old = r.get("oldest_unclaimed") or ""
        return (f"  ・未着手{n}件 {_subj(r)}"
                f"（起票:{who} / 最古の指示:{old[5:16].replace('T', ' ')} "
                f"経過{_elapsed(old)} / 話題全体は{r['status']}）")

    def _w(r):
        # 経過時間を必ず出す（15時間放置が一目で分かるように）。
        # 王命2026-08-22: 主数字は「最終応答からの経過」。無ければ従来どおり起票起点。
        #   両方出す(片方だけだと片方の嘘が生まれる):
        #     ・最終応答だけ → 返事が来た直後に見え、長期案件を見落とす
        #     ・起票だけ → 生きた案件が放置に見え取り下げ判断が出かける(実害2026-08-22)
        subj = (r.get("question") or r.get("subject") or "").replace("\n", " ")[:60]
        asked = r.get("asked_ts") or ""
        last = r.get("last_resp_ts") or ""
        label = ("形式外応答" if r.get("response_state") == "untyped_response"
                 else "質問")
        head = (f"  ・{r.get('asker','?')} → {r.get('askee','?')}【{label}】"
                f"{asked[5:16].replace('T',' ')} 「{subj}」")
        if last:
            by = r.get("last_resp_by") or ""
            askee = r.get("askee", "?")
            # 宛先本人が答えたのか、第三者が触っただけなのかを必ず言う。
            # 言わないと「くくるが返事した」と読める(2026-08-22の実害)。
            # 2026-09-26 ai-12ef: 本人かどうかは本体名で見る（表示は生の名のまま）。
            dare = (f"{by}本人が発言済み・answer未記録"
                    if by and _canon_name(by) == _canon_name(askee)
                    else f"{by}が発言・{askee}本人はまだ" if by
                    else "応答者不明")
            # 2026-09-05 修理(バグ探し中#9): 同じ「経過」という語で2つの時計を
            #   指していた(応答がある行=最終応答起点／無い行=起票起点)。語を分ける。
            #   静か=最後に誰かが糸に触れてからの時間 / 放置=質問した時からの時間。
            return (f"{head} 静か{_elapsed(last)}"
                    f"（放置{_elapsed(asked)} / 最終 "
                    f"{last[5:16].replace('T',' ')} {dare}）")
        soto = r.get("askee_last_ts") or ""
        if soto and soto > asked:
            # このスレッドに返事は無いが、宛先本人は円卓のどこかで口を開いている。
            # 「答えた」とは言わない。「沈黙ではない」とだけ言う（2026-08-22の実害の直し）。
            return (f"{head} 放置{_elapsed(asked)}"
                    f"（※このスレッドに返事は無いが、{r.get('askee','?')}は"
                    f"{soto[5:16].replace('T',' ')}に別の話題で発言＝沈黙ではない）")
        return f"{head} 放置{_elapsed(asked)}"


    def _onaji_wadai_de_tatamu(rows):
        """同じ差出人・同じ話題の未回答を1行に畳む(王命_20260906-0015 E「同一質問の束ね」)。

        なぜ要るか(2026-09-06 実測): 王の返答待ち7件のうち**4件が同じ台の毎朝の投稿**
        (世界の新着しらべ・3090に載る候補)だった。中身はほぼ同じで、王が答えるべきは
        実質1件。7件と表示すると、王は毎朝「7件も溜まっている」と受け取る。

        畳んでも**記録は消さない**。代表(最新)を出し、畳んだ物の日付を全部添える。
        並びは「グループの最も古い起票」で決める(代表は最新なので、代表の放置時間で
        並べると古い案件が下に沈む)。

        話題が空の行は畳まない(キーが違えば別グループになる=素通りする)。
        """
        kumi = {}
        junban = []
        for r in rows:
            # 2026-09-26 ai-12ef: 畳むキーは本体名（ChatGPT→Code と くくる→Code を同じ台の投稿として畳む）。
            key = (_canon_name(r.get("asker") or ""), _canon_name(r.get("askee") or ""),
                   r.get("topic") or "")
            if key not in kumi:
                kumi[key] = []
                junban.append(key)
            kumi[key].append(r)
        deki = []
        for key in junban:
            rs = sorted(kumi[key], key=lambda x: x.get("asked_ts") or "")
            deki.append((rs[-1], rs[:-1]))          # 代表=最新 / 畳んだ物=それより古い分
        deki.sort(key=lambda t: ((t[1][0] if t[1] else t[0]).get("asked_ts") or ""))
        return deki

    def _w_tatanda(daihyou, tatanda):
        gyou = _w(daihyou)
        if not tatanda:
            return gyou
        hizuke = " / ".join((r.get("asked_ts") or "")[5:16].replace("T", " ")
                            for r in tatanda)
        mottomo_furui = (tatanda[0].get("asked_ts") or "")
        wadai = daihyou.get("topic") or "同じ話題"
        return (gyou + "\n      （同じ話題「%s」であと%d件・%s。"
                "最も古い物は放置%s。畳んで表示している＝消えていない）"
                % (wadai, len(tatanda), hizuke, _elapsed(mottomo_furui)))

    loop_state = {}
    try:
        loop_state = json.loads((rt / "loop" / "state.json").read_text(encoding="utf-8"))
    except Exception:
        pass

    # ---- 進行中を「動いている／止まっている」で割る(2026-09-15 ネオ ai-31d6) ----
    # なぜ: 全窓の起動束へ載るのは digest の **頭1,200字だけ**(roundtable_brain.py の
    #   session_start: digest_chars=1200 → digest[:1200])。2026-09-15に実測すると、その
    #   1,200字に進行中の明細は **0件** しか入らず、19日前から止まっている未着手指示が
    #   862字(72%)を占め、末尾は「◆進行中（claim/waiting/blocke」と**文の途中で切れて**いた。
    #   ＝**全窓が最初に見る一枚に「今みんなが何をやっているか」が1件も無かった。**
    #   世界の作法も同じ所に着いている(needs-you first → 動いている物 → 止まった物は
    #   仕切って畳む＝Idle divider)。**要約はしない**(要約は必ず欠落する)。本人の題は
    #   そのまま、畳んだ物も件数と鍵で辿れる形に残す(黙って消さない・王命2026-08-02)。
    # 「動いている」の線 = 48時間。**王国が既に王命で使っている線に揃えた**
    #   （王命1222 6章「48時間規則」＝参加者宛の質問が48時間答え無しなら逃がし候補・2026-09-08実装、
    #     この関数の下にある◆逃がし候補と同じ線）。
    #   ⚠️2026-09-15に別窓 ai-b83d が実データで測り直した: 糸の中で次の投稿が来るまでの間隔
    #     3,580件で p50=0.1h / p90=11.0h / **p95=27.0h** / p99=196.7h。48h以内に次が来る=96.4%。
    #     統計だけなら p95(27h)がふさわしいが、**変えない**。理由=①同じ王国で「放置」の線を
    #     2つ持つ方が害が大きい ②データを見てから閾値を選ぶのは hindsight bias（世界の作法でも
    #     「閾値は事前に固定せよ」）。線を動かすなら、王命1222側と一緒に動かす事。
    UGOKU_BYOU = 48 * 3600

    def _keika_byou(ts):
        """最終更新からの経過秒。読めない時は None（＝止まっている側へ倒す）。"""
        from datetime import datetime as _d
        try:
            t = _d.fromisoformat(str(ts))
        except (ValueError, TypeError):
            return None
        return (_d.now(t.tzinfo) - t).total_seconds()

    ugoku, tomatta = [], []
    for _r in ip:
        _k = _keika_byou(_r.get("last_ts"))
        (ugoku if (_k is not None and _k < UGOKU_BYOU) else tomatta).append(_r)
    ugoku.sort(key=lambda r: r.get("last_ts") or "", reverse=True)   # 新しい順
    tomatta.sort(key=lambda r: r.get("last_ts") or "")               # 古い順(放置が長い物から)
    # 「本当に誰かの返事を待っている糸」の一覧。wk(王の返答待ち)と wr(返事を待っている人)は
    #   この関数の冒頭で既に取ってあるので、**新しい判定は作らない**。
    #   これが無いと、題に「【王の裁定待ち】…」と書いてある閉じ忘れの糸を、読む側が
    #   現役の待ちと誤読する(2026-09-15 09時に私自身が王へ誤報した)。
    machi_no_ito = {r.get("thread_id") for r in (list(wk) + list(wr)) if r.get("thread_id")}
    tomatta_machi = sum(1 for r in tomatta if r.get("thread_id") in machi_no_ito)
    # 未着手の生き死には下でも使う(計算はここへ集めた。判定は一切変えていない)。
    ikiteru = [r for r in uc if str(r.get("status")) not in ("complete", "cancelled")]
    nokorikasu = [r for r in uc if str(r.get("status")) in ("complete", "cancelled")]

    out = ["=== 王向け円卓ダイジェスト ==="]
    from ai_availability import read_shared
    from ai_availability import summary as availability_summary
    out.append(availability_summary(read_shared(rt.parent)))
    if loop_state:
        anomaly_names = ", ".join(
            item.get("title", "?") for item in loop_state.get("anomalies", [])[:4]) or "なし"
        deg_kazu, deg_saigo = _rireki_no_ijou(rt, 24)
        out.append(
            f"◆共有ループ状態: {loop_state.get('state', 'unknown')} "
            f"/ 実測:{loop_state.get('observed_at', '?')[:16]} "
            f"/ 異常:{anomaly_names}"
            + (f"　※**直近24時間に degraded の観測が{deg_kazu}回**"
               f"（最後 {deg_saigo[5:16].replace('T', ' ')}）。"
               "今 healthy でも、その間は外の道が落ちていた: roundtable/loop/history.jsonl"
               if deg_kazu else ""))
    else:
        out.append("◆共有ループ状態: 未計測（roundtable_loop.pyを実行）")
    wk_untyped = [row for row in wk if row.get("response_state") == "untyped_response"]
    wk_unanswered = [row for row in wk if row.get("response_state") != "untyped_response"]
    # 2026-09-06: 同じ差出人・同じ話題の物は1行に畳む(記録は消さない・下に日付を添える)。
    #   見出しの形「◆王の返答待ち: N件」は変えない(office_status.py と
    #   mainichi_no_koe.py が正規表現で読んでいる。実測で確認済み)。
    #   Nは**王が答えるべき件数**(畳んだ後の数)にする。生の件数も併記する。
    wk_tatanda = _onaji_wadai_de_tatamu(wk_unanswered)
    nama = len(wk_unanswered)
    out.append(f"◆王の返答待ち: {len(wk_tatanda)}件"
               + (f"（同じ話題を畳んだ後。畳む前は{nama}件）" if len(wk_tatanda) != nama else "")
               + "（放置=質問した時から / 静か=最後に誰かが糸に触れてから・放置が長い順）")
    out += [_w_tatanda(d, t) for d, t in wk_tatanda] or ["  （なし）"]
    # ◆今ひと目で（2026-09-15 ネオ ai-31d6）: 起動束へ載るのは頭1,200字だけなので、
    #   「王が答える物」の次に**全体の数**を置く。明細より先に数が来れば、切られても輪郭は届く。
    #   数は下の各節と同じ材料から数えているだけ＝新しい判定は1つも作っていない。
    _furui_mi = min((r.get("oldest_unclaimed") or "9999") for r in ikiteru) if ikiteru else ""
    out.append(
        f"◆今ひと目で: 動いている{len(ugoku)}件（48時間以内に更新） / "
        f"止まっている{len(tomatta)}件（うち誰かの返事待ちは{tomatta_machi}件"
        f"・残り{len(tomatta) - tomatta_machi}件は閉じ忘れ） / "
        f"まだ誰も着手していない{len(ikiteru)}話題"
        + (f"（最古 経過{_elapsed(_furui_mi)}）" if _furui_mi else "")
        + f" / 完了{len(cp)}件"
        "　※話題の全期間は roundtable_topic(話題, matome=True)、1件は roundtable_tadoru(鍵)")
    untyped = [row for row in wr if row.get("response_state") == "untyped_response"]
    unanswered = [row for row in wr if row.get("response_state") != "untyped_response"]
    out.append(f"◆返事を待っている人: {len(unanswered)}件")
    out += [_w(r) for r in unanswered] or ["  （なし）"]
    # 2026-09-08 ネオ(王命1222 6章「48時間規則」):
    #   「参加者宛の質問が48時間答え無しなら、ダイジェストで『逃がし候補』に載せ、
    #    Code が『別の者に振る／束ねる／閉じる』を決める（王には聞かない）」。
    #   **判定は1つも作り直していない** ―― 上の unanswered を48時間で切っているだけ。
    #   放置の起点は「質問した時」(asked_ts)。宛先本人が答えていない物だけがここに残る。
    from datetime import datetime as _dtN
    nigashi = []
    for _r in unanswered:
        try:
            _t = _dtN.fromisoformat(str(_r.get("asked_ts") or ""))
            _keika = (_dtN.now(_t.tzinfo) - _t).total_seconds()
        except Exception:
            continue
        if _keika >= 48 * 3600:
            nigashi.append(_r)
    out.append(f"◆逃がし候補（48時間以上、宛先本人の答えが無い）: {len(nigashi)}件"
               + ("　→ Codeが「別の者に振る／束ねる／閉じる」を決める（王には聞かない）"
                  if nigashi else ""))
    out += [_w(r) for r in nigashi] or ["  （なし）"]
    all_untyped = wk_untyped + untyped
    out.append(f"◆本人の発言はあるがanswer未記録: {len(all_untyped)}件")
    out += [_w(r) for r in all_untyped] or ["  （なし）"]
    # 王命2026-08-02「古い札の掛け替え」: 台帳で下ろした件数は黙って消さず1行残す。
    out.append(f"◆解決済み（台帳準拠・待ち行列から除外）: {len(resolved)}件")
    # 2026-08-20: 閉じた話題の中に残った古い指示は、状態機械上もう claim できない
    #   （完了済みthreadは終端）。それが毎回上に並ぶと、生きている未着手が埋もれる。
    #   判定は一切変えない（2026-08-02の実害の再発を避ける）。並べ方だけ、生きた話題を先に。
    #   ※ikiteru / nokorikasu の計算は上の集計ブロックへ移した(2026-09-15 ai-31d6・式は同じ)。
    #     「◆今ひと目で」でも同じ数を使うので、二度数えない。
    # 2026-08-22: 主数字を**生きている話題の数**にした。実測で5件中4件が閉じた話題で、
    #   「未着手5件」を誰も信じなくなっていた(狼少年)。閉じた物は消さず末尾に1行残す
    #   ——王命2026-08-02「古い札の掛け替え」に従い、黙って消さない。
    #   **判定(ucの作り方)は一切触っていない**(上のコメントの掟どおり)。
    # 2026-09-15 ネオ ai-31d6: 並びを「動いている → まだ誰も着手していない → 止まっている → 完了」へ。
    #   起動束へ載るのは頭1,200字だけで、今までは未着手862字で埋まり**動いている物が0件**だった。
    #   **判定も節の中身も1つも変えていない。順番と、止まった物の書き方だけ。**
    out.append(f"◆進行中・動いている（48時間以内に更新）: {len(ugoku)}件"
               f"（進行中は全部で{len(ip)}件）")
    out += [_l(r) for r in ugoku] or ["  （なし）"]
    out.append(f"◆まだ誰も着手していない指示: {len(ikiteru)}話題")
    out += [_u(r) for r in ikiteru] or ["  （なし）"]
    if nokorikasu:
        furui = min((r.get("oldest_unclaimed") or "9999") for r in nokorikasu)
        out.append(f"  （ほかに、閉じた話題に残った古い指示が{len(nokorikasu)}話題ある。最古 {furui[5:16].replace('T', ' ')}・経過{_elapsed(furui)}。掃除するなら言ってくれ）")
    out.append(f"◆進行中・止まっている（48時間以上、誰も触っていない）: {len(tomatta)}件"
               + ("　※鍵で1件を開ける: roundtable_tadoru(鍵)" if tomatta else ""))
    out += [_tomatta_gyou(r) for r in tomatta] or ["  （なし）"]
    out.append(f"◆完了: {len(cp)}件"); out += [_l(r) for r in cp[-10:]] or ["  （なし）"]
    return "\n".join(out)


# ---------------- Markdown取り込み（backfill・冪等・batchなので走査可） ----------------

_MD_ENTRY = re.compile(r"(?m)^## \[(\d{2}:\d{2})\]\s*(.+?)\s*→\s*(.+?)\s*$")


def backfill_markdown(rt) -> dict:
    files = sorted(rt.glob("円卓_*.md"))
    imported = skipped = 0
    for f in files:
        day = f.stem.replace("円卓_", "")
        text = f.read_text(encoding="utf-8")
        ms = list(_MD_ENTRY.finditer(text))
        for i, m in enumerate(ms):
            hm, sender, to = m.group(1), m.group(2).strip(), m.group(3).strip()
            body = text[m.end():(ms[i + 1].start() if i + 1 < len(ms) else len(text))].strip("\n")
            # 日ごとmdは「目次1行」だけを持つ（王命2026-07-29・本文は 話題/*.md）。
            # 目次行を取り込むと本文と二重記録になるため読み飛ばす。
            if body.lstrip().startswith("\u2192 \u8a71\u984c/"):
                skipped += 1
                continue
            if not body.strip() or not valid_sender(sender):
                skipped += 1
                continue
            try:
                ev = emit(rt, sender=sender, to=to, content=body, type="note",
                          source="markdown", at_day=day, at_hm=hm)
            except RTError:
                skipped += 1
                continue
            imported += (0 if ev.get("_dedup") else 1)
            skipped += (1 if ev.get("_dedup") else 0)
    return {"imported": imported, "skipped_or_dup": skipped}


# ---------------- 非破壊 backup / restore（Codex P0-4） ----------------

def backup(rt, dest_dir=None) -> dict:
    """events/ + cursors.json を tar.gz へ退避。manifest.json（sha256）を**tar内に同梱**。
    events lock を取り整合snapshotにする（Codex P1-F）。破壊しない。"""
    _ensure_dirs(rt)
    stamp = time.strftime("%Y%m%d-%H%M%S", time.localtime())
    dest = Path(dest_dir) if dest_dir else (_index_dir(rt) / "backups")
    dest.mkdir(parents=True, exist_ok=True)
    # 秒内の連続backupでも既存証跡を上書きしない。
    tar_path = dest / (
        f"rtv2_backup_{stamp}-{time.time_ns()}-{uuid.uuid4().hex[:8]}.tar.gz")
    with _events_lock(rt).open("w") as elk, _cursors_lock(rt).open("w") as clk:
        fcntl.flock(elk, fcntl.LOCK_EX)       # 整合snapshot（emitと排他）
        fcntl.flock(clk, fcntl.LOCK_EX)
        try:
            canonical_events = list(
                _iter_events_unlocked(rt, _log=True, _strict=True))
            _validate_event_sequence(canonical_events)
            members = list(_events_files(rt))
            cf = _cursors_file(rt)
            if cf.exists():
                members.append(cf)
            manifest = {"created": time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime()),
                        "files": {}, "event_count": len(canonical_events)}
            with tarfile.open(tar_path, "x:gz") as tar:
                for f in members:
                    data = f.read_bytes()
                    manifest["files"][f.name] = {"sha256": hashlib.sha256(data).hexdigest(),
                                                 "bytes": len(data)}
                    tar.add(str(f), arcname=f.name)
                mbytes = json.dumps(manifest, ensure_ascii=False, indent=2).encode("utf-8")
                info = tarfile.TarInfo("manifest.json"); info.size = len(mbytes)
                import io
                tar.addfile(info, io.BytesIO(mbytes))
            os.chmod(tar_path, 0o600)
            with tar_path.open("rb") as saved:
                os.fsync(saved.fileno())
            _fsync_dir(dest)
        finally:
            fcntl.flock(clk, fcntl.LOCK_UN)
            fcntl.flock(elk, fcntl.LOCK_UN)
    return {"backup": str(tar_path), "files": len(members),
            "event_count": manifest["event_count"], "manifest_ok": True}


def restore(rt, tar_path) -> dict:
    """backup から復元。**manifest.json の sha256 を全件照合**し、一致した時だけ差し替える。
    現行を巻き戻さず、日別JSONLがbyte-prefix関係の時だけappend-onlyに統合する。
    全日分を同一FS stageへ作り、原子directory exchangeを1回だけ行う。"""
    import shutil as _sh
    import tempfile as _tf
    _ensure_dirs(rt)
    with tarfile.open(tar_path, "r:gz") as tar:
        names = tar.getnames()
        if "manifest.json" not in names:
            raise RTError("restore拒否: manifest.json が無い（integrity検証不可）")
        manifest = json.loads(tar.extractfile("manifest.json").read().decode("utf-8"))
        declared = set(manifest.get("files", {}))
        data_members = [m for m in tar.getmembers() if m.name != "manifest.json"]
        archived = {m.name for m in data_members}
        if archived != declared:
            raise RTError("restore拒否: manifestとarchiveのファイル一覧が一致しません")
        for name in declared:
            if not (re.fullmatch(r"events_\d{8}\.jsonl", name) or name == "cursors.json"):
                raise RTError(f"restore拒否: 許可されないファイル名です: {name}")

        staged = Path(_tf.mkdtemp(prefix=".restore-stage-", dir=str(rt)))
        raw = staged / "raw"
        backup_events = staged / "backup_events"
        merged_events = staged / "merged_events"
        raw.mkdir(); backup_events.mkdir(); merged_events.mkdir()
        try:
            for m in data_members:
                if not m.isfile() or "/" in m.name or "\\" in m.name or ".." in m.name:
                    raise RTError(f"restore拒否: 不正なarchive memberです: {m.name}")
                tar.extract(m, path=str(raw), filter="data")
            # sha256照合（Codex P1-F: 改ざん/不一致は拒否）
            backup_count = 0
            for name, meta in manifest.get("files", {}).items():
                fp = raw / name
                if not fp.exists():
                    raise RTError(f"restore拒否: manifest記載の {name} がbackupに無い")
                got = hashlib.sha256(fp.read_bytes()).hexdigest()
                if got != meta.get("sha256"):
                    raise RTError(f"restore拒否: {name} のsha256不一致（改ざんの疑い）")
                if name.endswith(".jsonl"):
                    _assert_complete_jsonl_tail(fp)
                    with fp.open("r", encoding="utf-8") as inp:
                        for lineno, line in enumerate(inp, 1):
                            try:
                                candidate = json.loads(line)
                            except json.JSONDecodeError as exc:
                                raise RTError(
                                    f"restore拒否: {name}:{lineno} が不正JSONです") from exc
                            ok, reason = _valid_event(candidate)
                            if not ok:
                                raise RTError(
                                    f"restore拒否: {name}:{lineno} のschema不正: {reason}")
                            backup_count += 1
                    os.replace(fp, backup_events / name)
            if backup_count != int(manifest.get("event_count", backup_count)):
                raise RTError(
                    f"restore拒否: manifest event_count不一致 "
                    f"({manifest.get('event_count')} != {backup_count})")

            # 全件一致 → lock取得後のlive snapshotとappend-only統合。
            stamp = time.strftime("%Y%m%d-%H%M%S", time.localtime())
            rollback_root = (_index_dir(rt) / "restore_rollbacks" /
                             f"{stamp}-{uuid.uuid4().hex[:8]}")
            rollback_root.mkdir(parents=True)
            rollback_events = rollback_root / "events"
            rollback_cursor = rollback_root / "cursors.json"
            extra_projection_ids = []
            cursor_policy = "unchanged"
            with (_pipeline_lock(rt).open("w") as plk,
                  _restore_lock(rt).open("w") as rlk,
                  _rebuild_lock(rt).open("w") as blk,
                  _events_lock(rt).open("w") as elk,
                  _cursors_lock(rt).open("w") as clk):
                fcntl.flock(plk, fcntl.LOCK_EX)
                fcntl.flock(rlk, fcntl.LOCK_EX)
                fcntl.flock(blk, fcntl.LOCK_EX)
                fcntl.flock(elk, fcntl.LOCK_EX)
                fcntl.flock(clk, fcntl.LOCK_EX)
                exchanged = old_archived = False
                created_projection_markers = []
                dirty_was = _dirty_path(rt).exists()
                cursor_before = (_cursors_file(rt).read_bytes()
                                 if _cursors_file(rt).exists() else None)
                cursor_reset = False
                try:
                    live_by_name = {p.name: p for p in _events_files(rt)}
                    backup_by_name = {p.name: p for p in backup_events.glob("events_*.jsonl")}
                    for name in sorted(set(live_by_name) | set(backup_by_name)):
                        live_path = live_by_name.get(name)
                        backup_path = backup_by_name.get(name)
                        live_data = live_path.read_bytes() if live_path else b""
                        backup_data = backup_path.read_bytes() if backup_path else b""
                        if live_path and backup_path:
                            if backup_data.startswith(live_data):
                                chosen = backup_data       # backupが現行の前進
                            elif live_data.startswith(backup_data):
                                chosen = live_data         # 古いbackupで巻き戻さない
                            else:
                                raise RTError(
                                    f"restore拒否: append-only履歴が分岐しています: {name}")
                        else:
                            chosen = live_data if live_path else backup_data
                        dest = merged_events / name
                        with dest.open("xb") as out:
                            out.write(chosen); out.flush(); os.fsync(out.fileno())
                        os.chmod(dest, 0o600)

                    # 統合世代を厳格検証し、event_id重複も切替前に拒否する。
                    live_ids = {e["event_id"] for e in _iter_events_unlocked(rt, _log=False)}
                    merged_ids = set()
                    merged_dedup = {}
                    merged_requests = {}
                    slug_threads = {}
                    thread_slugs = {}
                    merged_sequence = []
                    for path in sorted(merged_events.glob("events_*.jsonl")):
                        _assert_complete_jsonl_tail(path)
                        with path.open("r", encoding="utf-8") as inp:
                            for lineno, line in enumerate(inp, 1):
                                try:
                                    ev = json.loads(line)
                                except json.JSONDecodeError as exc:
                                    raise RTError(
                                        f"restore拒否: 統合後 {path.name}:{lineno} が不正JSON") from exc
                                ok, reason = _valid_event(ev)
                                if not ok:
                                    raise RTError(
                                        f"restore拒否: 統合後 {path.name}:{lineno} schema不正: {reason}")
                                if ev["event_id"] in merged_ids:
                                    raise RTError(
                                        f"restore拒否: event_id重複: {ev['event_id']}")
                                merged_ids.add(ev["event_id"])
                                merged_sequence.append(ev)
                                dedup = ev.get("dedup_key", "")
                                if dedup:
                                    if dedup in merged_dedup:
                                        raise RTError(
                                            f"restore拒否: dedup_key競合: {dedup} "
                                            f"({merged_dedup[dedup]} / {ev['event_id']})")
                                    merged_dedup[dedup] = ev["event_id"]
                                request = ev.get("request_id", "")
                                if request:
                                    if request in merged_requests:
                                        raise RTError(
                                            f"restore拒否: request_id競合: {request}")
                                    merged_requests[request] = ev["event_id"]
                                slug = ev.get("topic_slug", "")
                                projected = _requires_projection(ev)
                                if slug and projected:
                                    slug_threads.setdefault(slug, set()).add(ev["thread_id"])
                                    thread_slugs.setdefault(ev["thread_id"], set()).add(slug)
                    bad_slugs = {k: v for k, v in slug_threads.items() if len(v) > 1}
                    bad_threads = {k: v for k, v in thread_slugs.items() if len(v) > 1}
                    if bad_slugs or bad_threads:
                        raise RTError(
                            "restore拒否: topic_slug/threadの全体対応が交差しています")
                    _validate_event_sequence(merged_sequence)
                    extra_event_ids = merged_ids - live_ids
                    # 旧markdown/direct eventはtopic/slug情報が無く、機械的に
                    # 「雑談」へ再投影すると複数threadが衝突する。明示済みの新eventと、
                    # projection_required追加前に常時投影していたpost/MCPだけを回復する。
                    extra_projection_ids = sorted(
                        ev["event_id"] for ev in merged_sequence
                        if ev["event_id"] in extra_event_ids
                        and _requires_projection(ev))
                    if merged_events.stat().st_dev != _events_dir(rt).stat().st_dev:
                        raise RTError("restore stageとliveが別filesystemです")
                    _fsync_dir_required(merged_events)

                    # 復元eventの投影markerもexchangeより先に全件fsyncする。
                    # exchange直後のpower lossでもMarkdown回復対象を失わない。
                    for event_id in extra_projection_ids:
                        _write_marker(_views_dirty_path(rt) / event_id,
                                      "restore_projection_preexchange\n", required=True)
                        created_projection_markers.append(event_id)

                    # 過去時刻の復元eventが既存cursorより前へ入る場合、既読扱いで
                    # 永久に隠れないよう全participantをfail-openへ戻す。
                    if extra_event_ids:
                        if cursor_before is not None:
                            _replace_bytes_atomic(rollback_cursor, cursor_before)
                        _replace_bytes_atomic(_cursors_file(rt), b"{}")
                        cursor_reset = True
                        cursor_policy = "fail_open_reset"

                    # dirtyをexchangeより先に耐久化。crash後は必ず新liveからrebuildされる。
                    _set_dirty(rt, "restore_exchange_pending\n", required=True)
                    _fsync_dir_required(_index_dir(rt))
                    _rename_exchange(_events_dir(rt), merged_events)
                    exchanged = True
                    _fsync_dir_required(rt)

                    # exchange後のmerged_eventsは旧live。削除せずrollback証跡へ保持。
                    os.replace(merged_events, rollback_events)
                    old_archived = True
                    _fsync_dir_required(rollback_root)
                    _fsync_dir_required(staged)
                except Exception as exc:
                    try:
                        if exchanged:
                            if old_archived:
                                os.replace(rollback_events, merged_events)
                                old_archived = False
                            _rename_exchange(_events_dir(rt), merged_events)
                            _fsync_dir_required(rt)
                        for event_id in created_projection_markers:
                            _remove_marker(_views_dirty_path(rt) / event_id)
                        if cursor_reset:
                            if cursor_before is None:
                                _remove_marker(_cursors_file(rt))
                            else:
                                _replace_bytes_atomic(_cursors_file(rt), cursor_before)
                        if not dirty_was:
                            _clear_dirty(rt)
                    except Exception as rollback_exc:
                        _set_dirty(rt, "restore_rollback_failed\n", required=True)
                        raise RTError(
                            f"restore失敗後のrollbackにも失敗。新規受付停止: "
                            f"original={exc}; rollback={rollback_exc}; "
                            f"old_generation={merged_events}") from rollback_exc
                    raise
                finally:
                    fcntl.flock(clk, fcntl.LOCK_UN)
                    fcntl.flock(elk, fcntl.LOCK_UN)
                    fcntl.flock(blk, fcntl.LOCK_UN)
                    fcntl.flock(rlk, fcntl.LOCK_UN)
                    fcntl.flock(plk, fcntl.LOCK_UN)
        finally:
            _sh.rmtree(staged, ignore_errors=True)
    stats = rebuild_index(rt)
    if stats["events"] != len(merged_ids) or stats.get("dedup_skipped"):
        _set_dirty(rt, "restore_rebuild_count_mismatch\n", required=True)
        raise RTError(
            f"restore後索引件数が正本と不一致: JSONL={len(merged_ids)} "
            f"SQLite={stats['events']} dedup_skipped={stats.get('dedup_skipped')}")
    return {"restored_events": stats["events"], "verified": True,
            "rollback": str(rollback_root),
            "projection_pending": len(extra_projection_ids),
            "cursor_policy": cursor_policy}


# ---------------- CLI ----------------

def _print(obj):
    print(json.dumps(obj, ensure_ascii=False, indent=2))


def _mado_ga_nai_to_tsugeru(sender, mado) -> None:
    """Code 名義で窓の名が無い時、止めずに一言だけ告げる(投稿は通す)。

    Code の窓は Mac の hook(mado_wo_kikai_ga_ireru.py)が --mado を機械で足すので、
    ここへ来るのは機械の Code 名義か、hook を通らない道。どちらも止めない。
    """
    if sender == "Code" and not (mado or "").strip():
        print("ℹ️ 窓の名(--mado ai-xxxx)が無い投稿です。Code の窓なら付けると"
              "コックピットで本人の跡に数えられます。", file=sys.stderr)


def main(argv=None):
    p = argparse.ArgumentParser(description="円卓V2 イベント台帳")
    sub = p.add_subparsers(dest="cmd", required=True)

    def common(sp):
        sp.add_argument("--rt", default=None)

    sp = sub.add_parser("emit"); common(sp)
    sp.add_argument("--sender", required=True); sp.add_argument("--to", default="全員")
    sp.add_argument("--content", required=True); sp.add_argument("--type", default="note")
    sp.add_argument("--thread", default=None); sp.add_argument("--reply-to", default=None)
    sp.add_argument("--report-path", default=""); sp.add_argument("--dedup-key", default=None)
    sp.add_argument("--request-id", default=None)
    sp.add_argument("--source", default="v2")
    sp.add_argument("--at-day", default=None); sp.add_argument("--at-hm", default=None)
    sp.add_argument("--topic", default=None); sp.add_argument("--proof", default="未検証")
    # 2026-09-07 ネオ: 既定を「投影する」に変えた。素の emit(source=v2)が話題mdへ出ず、
    #   今日だけで37件が未投影だった(円卓へ流したかの関が鳴り続けた・王が読む話題mdから抜けた)。
    #   post.sh の --projection-required はそのまま通る。投影したくない時だけ --no-projection。
    sp.add_argument("--projection-required", dest="projection_required", action="store_true", default=True)
    sp.add_argument("--no-projection", dest="projection_required", action="store_false",
                    help="話題mdへ投影しない(JSONL/SQLiteだけ)")
    sp.add_argument("--terminal-once", action="store_true")
    sp.add_argument("--mado", default=os.environ.get("MADO", ""),
                    help="窓の名(例 ai-61da)。sender は AI の種類、mado はその窓(案件 jiku5f 名義の分離)")

    sp = sub.add_parser("materialize"); common(sp)
    sp.add_argument("--event-id", required=True)
    sp.add_argument("--defer-clear", action="store_true")
    sp = sub.add_parser("projection-clear"); common(sp)
    sp.add_argument("--event-id", required=True)
    sp = sub.add_parser("recover-projections"); common(sp)   # 2026-09-05 中#10/低#10
    sp.add_argument("--no-index", action="store_true")
    sp = sub.add_parser("event-exists"); common(sp)
    sp.add_argument("--event-id", required=True)

    for name in ("claim", "progress", "complete", "blocked"):
        sp = sub.add_parser(name); common(sp)
        sp.add_argument("--sender", required=True); sp.add_argument("--thread", required=True)
        sp.add_argument("--content", default=name); sp.add_argument("--to", default="全員")
        # 2026-09-15 ネオ: 証拠を渡す欄が emit にしか無く、実測で脳の89.4%が証拠を
        # 指していなかった(別窓 ai-20)。書き忘れではなく**書く欄が無かった**。
        sp.add_argument("--report-path", default="")
        if name == "claim":
            sp.add_argument("--reply-to", default=None)
        sp.add_argument("--mado", default=os.environ.get("MADO", ""),
                        help="窓の名(例 ai-61da)。emit と同じ。無いとコックピットで本人の跡に数えられない")

    sp = sub.add_parser("answer"); common(sp)
    sp.add_argument("--sender", required=True); sp.add_argument("--reply-to", required=True)
    sp.add_argument("--content", required=True); sp.add_argument("--to", default="全員")
    sp.add_argument("--report-path", default="")
    sp.add_argument("--dedup-key", default="")
    sp.add_argument("--mado", default=os.environ.get("MADO", ""),
                    help="窓の名(例 ai-61da)。emit と同じ")

    for name in ("rebuild", "backfill"):
        sp = sub.add_parser(name); common(sp)
    sp = sub.add_parser("backup"); common(sp); sp.add_argument("--dest", default=None)
    sp = sub.add_parser("restore"); common(sp); sp.add_argument("tar_path")

    sp = sub.add_parser("inbox"); common(sp)
    sp.add_argument("--participant", required=True)
    sp.add_argument("--all", action="store_true"); sp.add_argument("--advance", action="store_true")
    sp.add_argument("--latest", action="store_true", help="新しい順に返す")
    sp.add_argument("--limit", type=int, default=None, help="返す件数。0は無制限")
    sp.add_argument("--since", default="", help="YYYY-MM-DDまたはISO 8601以降")
    sp.add_argument("--type", action="append", choices=TYPES, dest="event_types")
    sp.add_argument("--status", action="append", choices=STATUSES, dest="statuses")
    sp.add_argument("--direct", action="store_true", help="全員宛を除き本人への直接宛だけ")
    sp.add_argument("--pending", action="store_true", help="未回答質問と未claim指示だけ")
    sp = sub.add_parser("read"); common(sp); sp.add_argument("--participant", required=True)
    sp = sub.add_parser("view"); common(sp)
    sp.add_argument("kind", choices=["in_progress", "waiting_king", "waiting_reply", "unclaimed", "complete", "unread"])
    sp.add_argument("--participant", default=None)
    sp = sub.add_parser("thread"); common(sp); sp.add_argument("thread_id")
    sp = sub.add_parser("digest"); common(sp)
    # 王命2026-08-02「古い札の掛け替え」: 質問→回答の対を台帳に追記する。
    # 追記のみ・削除不可。resolutions.jsonl に1行足す。
    sp = sub.add_parser("resolve"); common(sp)
    sp.add_argument("--question-event-id", required=True)
    sp.add_argument("--answer-event-id", required=True)
    sp.add_argument("--resolved-by", required=True)
    sp.add_argument("--note", default="")
    sp = sub.add_parser("resolutions"); common(sp)
    # post.sh がMarkdown追記前に本文を伏字するための素通しフィルタ（Codex P0-B）
    sp = sub.add_parser("redact"); sp.add_argument("--content", required=True)

    a = p.parse_args(argv)
    rt = Path(a.rt) if getattr(a, "rt", None) else rt_dir()
    try:
        if a.cmd == "emit":
            result = emit(rt, sender=a.sender, to=a.to, content=a.content, type=a.type,
                          thread_id=a.thread, reply_to=a.reply_to, report_path=a.report_path,
                          dedup_key=a.dedup_key, request_id=a.request_id, source=a.source,
                          at_day=a.at_day, at_hm=a.at_hm, topic=a.topic, proof=a.proof,
                          projection_required=a.projection_required,
                          terminal_once=a.terminal_once, mado=a.mado)
            _print(result)
            if result.get("_index_dirty"):
                return 4
            # 投影は emit の出口で1回(post.sh と同じ道=recover_projections 1本化・2026-09-05)。
            # 失敗しても marker が残るので次の回収で拾われる(fail-open・止めない)。
            # 2026-10-03 ネオ ai-73b1(王命_20261003-1321): roundtable_post.sh は emit の後で自分で
            #   materialize→索引→projection-clear を段ごとの終了コードつきで行う。ここでも回すと
            #   1投稿で投影と索引が2回ずつ走り(実測 emit 7.3秒のうち約5.4秒)、その間パイプラインの錠を
            #   握り続けていた。post.sh が RT_PROJECTION_BY_CALLER=1 を付けた時だけ、ここは任せる。
            #   直接 CLI で emit を打つ人・cron は今までどおりここで投影される(marker は残るので取りこぼさない)。
            if (a.projection_required and not result.get("_dedup")
                    and os.environ.get("RT_PROJECTION_BY_CALLER") != "1"):
                try:
                    recover_projections(rt)
                except Exception as exc:
                    print(f"⚠️ 話題mdへの投影は次の回収に回した: {exc}", file=sys.stderr)
        elif a.cmd == "materialize":
            _print(materialize_markdown(rt, a.event_id, defer_clear=a.defer_clear))
        elif a.cmd == "projection-clear":
            _print(clear_projection_marker(rt, a.event_id))
        elif a.cmd == "recover-projections":
            rec = recover_projections(rt, run_index=not a.no_index)
            if rec["stale_cleared"]:
                print(f"ℹ️ JSONL未受理のstale投影markerを解除: {', '.join(rec['stale_cleared'])}", file=sys.stderr)
            if rec["recovered"]:
                print(f"ℹ️ 未投影event {len(rec['recovered'])}件を回復しました", file=sys.stderr)
            for eid, reason in rec["quarantined"]:
                print(f"⚠️ 投影できないeventを退避(.views_quarantine・人手で確認): {eid}: {reason}", file=sys.stderr)
            _print(rec)
            if rec["index_error"]:
                print(f"❌ 前回eventの話題索引回復に失敗: {rec['index_error']}", file=sys.stderr)
                return 14
        elif a.cmd == "event-exists":
            _print(_find_event(rt, a.event_id))
        elif a.cmd in ("claim", "progress", "complete", "blocked"):
            _mado_ga_nai_to_tsugeru(a.sender, a.mado)
            result = _post_pipeline_call(
                rt, sender=a.sender, to=a.to, content=a.content,
                type=a.cmd, thread_id=a.thread,
                reply_to=(a.reply_to if a.cmd == "claim" else ""), topic=None,
                report_path=getattr(a, "report_path", "") or "", mado=a.mado)
            _print(result)
        elif a.cmd == "answer":
            _mado_ga_nai_to_tsugeru(a.sender, a.mado)
            result = _post_pipeline_call(
                rt, sender=a.sender, to=a.to, content=a.content,
                type="answer", reply_to=a.reply_to, topic=None,
                report_path=getattr(a, "report_path", "") or "", mado=a.mado,
                dedup_key=a.dedup_key)
            _print(result)
        elif a.cmd == "rebuild":
            _print(rebuild_index(rt))
        elif a.cmd == "backfill":
            _print(backfill_markdown(rt))
        elif a.cmd == "backup":
            _print(backup(rt, a.dest))
        elif a.cmd == "restore":
            _print(restore(rt, a.tar_path))
        elif a.cmd == "inbox":
            if a.limit is not None and a.limit < 0:
                raise RTError("--limit は0以上で指定してください")
            # 旧 `--all` は無制限を維持。通常の未読取得は従来どおり末尾50件。
            inbox_limit = a.limit if a.limit is not None else (0 if a.all else 50)
            _print(inbox(
                rt, a.participant, unread_only=not a.all, limit=inbox_limit,
                newest_first=a.latest, since=a.since,
                event_types=a.event_types, statuses=a.statuses,
                direct_only=a.direct, pending_only=a.pending,
            ))
            if a.advance:
                advance_cursor(rt, a.participant)
        elif a.cmd == "read":
            _print({"cursor": advance_cursor(rt, a.participant)})
        elif a.cmd == "view":
            _print(view(rt, a.kind, participant=a.participant))
        elif a.cmd == "thread":
            _print(thread_history(rt, a.thread_id))
        elif a.cmd == "digest":
            print(digest(rt))
        elif a.cmd == "resolve":
            _print(resolve_question(
                rt,
                question_event_id=a.question_event_id,
                answer_event_id=a.answer_event_id,
                resolved_by=a.resolved_by,
                note=a.note,
            ))
        elif a.cmd == "resolutions":
            _print(list(_load_resolutions(rt).values()))
        elif a.cmd == "redact":
            sys.stdout.write(redact_secrets(a.content))
    except RTError as e:
        print(f"RTError: {e}", file=sys.stderr)
        return 3
    return 0


if __name__ == "__main__":
    sys.exit(main())
