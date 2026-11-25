#!/usr/bin/env python3
"""ニライBrain 検索順位進化コア v2（2026-11-21: 機械JSON退避 + Listwise同型/飽和合意 + 仮説A骨格/時代断層強化 + 超飽和検証証跡タイブレーク）。
2026-11-22 第5波 B4_Guarded: Listwise 先頭トークン Logprob 正逆確率平均融合 (W=2.5, margin=1.5) + 2汎用安全ガード。
2026-11-23 第7波 Qwen3-Reranker-0.6B: 接戦時 Top-5 へゼロベース確率単体正規化 (sum<=1.0, W=2.5, fail-open) を B4 と対称同時加算。
2026-11-24 第13波 Track A: Stage-3 数学的厳密枝刈り (Qwen0.6B -> Check1 -> B4 Fwd -> Check2 -> B4 Rev, 既定ON・精度劣化0%保証)。"""
from __future__ import annotations

import http.client
import functools
import json
import math
import os
import re
import sys
import threading
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

# 2026-11-24 速度第1波(Phase A): HTTP Keep-Alive 永続接続プール & URL解決キャッシュ(厳密等価・追加VRAM 0MiB)
_ORIG_URLOPEN = urllib.request.urlopen
_HTTP_TLS = threading.local()
_DAN3_URL_CACHE_LOCK = threading.RLock()
_DAN3_URL_CACHE: dict[str, tuple[float, str]] = {}
_DAN3_URL_TTL_SEC = 30.0


def _http_keepalive_ari() -> bool:
    return os.environ.get("KINGDOM_HTTP_KEEPALIVE", "1").strip().lower() not in ("0", "false", "no", "off")


def _http_post_json(url: str, body: bytes, timeout: float) -> dict:
    """スレッドローカル HTTPConnection 再利用による Keep-Alive JSON POST。
    単体テスト等で urllib.request.urlopen がモックされている場合や Keep-Alive 無効時は
    従来どおり urllib.request.urlopen へ透過フォールバックする。
    """
    if (not _http_keepalive_ari()) or (urllib.request.urlopen is not _ORIG_URLOPEN):
        req = urllib.request.Request(
            url, data=body, headers={"Content-Type": "application/json"}
        )
        with urllib.request.urlopen(req, timeout=timeout) as res:
            return json.loads(res.read().decode("utf-8"))

    parsed = urllib.parse.urlsplit(url)
    scheme = parsed.scheme.lower()
    if scheme not in ("http", "https") or not parsed.hostname:
        req = urllib.request.Request(
            url, data=body, headers={"Content-Type": "application/json"}
        )
        with urllib.request.urlopen(req, timeout=timeout) as res:
            return json.loads(res.read().decode("utf-8"))

    host = parsed.hostname
    port = parsed.port or (443 if scheme == "https" else 80)
    target = parsed.path or "/"
    if parsed.query:
        target = f"{target}?{parsed.query}"
    key = (scheme, host, port)
    pool: dict | None = getattr(_HTTP_TLS, "pool", None)
    if pool is None:
        pool = {}
        _HTTP_TLS.pool = pool

    headers = {
        "Content-Type": "application/json",
        "Connection": "keep-alive",
    }
    last_exc: Exception | None = None
    for attempt in range(2):
        conn = pool.get(key)
        reused = conn is not None
        if conn is None:
            cls = http.client.HTTPSConnection if scheme == "https" else http.client.HTTPConnection
            conn = cls(host, port, timeout=timeout)
            pool[key] = conn
        else:
            conn.timeout = timeout
            if conn.sock is not None:
                try:
                    conn.sock.settimeout(timeout)
                except OSError:
                    pass
        try:
            conn.request("POST", target, body=body, headers=headers)
            res = conn.getresponse()
            raw = res.read()
            if res.will_close:
                try:
                    conn.close()
                except Exception:
                    pass
                pool.pop(key, None)
            if res.status < 200 or res.status >= 300:
                raise RuntimeError(f"HTTP {res.status}")
            return json.loads(raw.decode("utf-8"))
        except (
            http.client.RemoteDisconnected,
            http.client.BadStatusLine,
            http.client.CannotSendRequest,
            http.client.ResponseNotReady,
            BrokenPipeError,
            ConnectionResetError,
        ) as exc:
            last_exc = exc
            try:
                conn.close()
            except Exception:
                pass
            pool.pop(key, None)
            if reused and attempt == 0:
                continue
            raise
        except Exception:
            try:
                conn.close()
            except Exception:
                pass
            pool.pop(key, None)
            raise
    if last_exc is not None:
        raise last_exc
    raise RuntimeError("http_post_json failed")

_SCRIPTS_DIR = Path(__file__).resolve().parent
if str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))

from brain_text_utils import item_key, normalize, query_terms

RERANK_URL = os.environ.get("KINGDOM_RERANK", "http://127.0.0.1:8794/v1/rerank")
RERANK_TIMEOUT = 20
RERANK_QWEN06B_URL = os.environ.get("KINGDOM_RERANK_QWEN06B", "http://127.0.0.1:8794/v1/rerank_qwen06b")


def _iso_datetime(value: str) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(raw)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


# ===========================================================================
# 2026-11-21 進化コア: ルートA-1(3窓純本文MaxP + 接戦正逆Listwise) & ルートA-3(仮説A/B)
# ===========================================================================
DAN2_PURE_MAXP_TOPK = 20
DAN2_PURE_MAXP_MIN_LEN = 800
DAN2_PURE_MAXP_WIN = 1000
DAN2_PURE_MAXP_STRIDE = 400
DAN2_PURE_MAXP_WEIGHT = 0.35
DAN3_LISTWISE_MARGIN = 1.5
DAN3_LISTWISE_TOPK = 5
DAN3_LISTWISE_MODEL_DEFAULT = "qwen2.5:7b"
DAN3_LISTWISE_BOOST_DEFAULT = 1.8
DAN3_LISTWISE_TIMEOUT_DEFAULT = 25.0
DAN3_LISTWISE_LOGPROB_WEIGHT_DEFAULT = 2.5
DAN3_LISTWISE_TOP_LOGPROBS = 20
DAN3_QWEN06B_WEIGHT_DEFAULT = 2.5
DAN3_QWEN06B_TIMEOUT_DEFAULT = 15.0
DAN3_QWEN06B_MAX_CHARS = 2400
DAN3_EXACT_PRUNE_EPS = 1e-4
DAN3_EXACT_PRUNE_MODE_DEFAULT = "top1_top3"
_DAN3_EXACT_PRUNE_VALID_MODES = ("top1", "top1_top3", "full")


def _env_flag_on(name: str, default: str = "1") -> bool:
    return os.environ.get(name, default).strip().lower() not in ("0", "false", "no", "off")


def dan2_pure_maxp_ari() -> bool:
    """2段目後の上位20件長文に対する3窓純本文MaxPを有効にするか(既定ON)。"""
    return _env_flag_on("KINGDOM_DAN2_PURE_MAXP", "1")


def dan3_listwise_ari() -> bool:
    """接戦時(top1-top2<=2.2)の上位5件正逆Listwiseコンセンサスを有効にするか(既定ON)。"""
    return _env_flag_on("KINGDOM_DAN3_LISTWISE", "1")


def dan3_listwise_logprob_ari() -> bool:
    """Listwise で先頭トークン Logprob 正逆確率平均融合を使うか(既定ON)。OFF時は従来の正逆添字合意。"""
    return _env_flag_on("KINGDOM_DAN3_LOGPROB", "1")


def dan3_qwen06b_ari() -> bool:
    """接戦時 Top-5 で Qwen3-Reranker-0.6B 非負確率融合を B4 と併用するか(既定ON)。"""
    return _env_flag_on("KINGDOM_DAN3_QWEN06B", "1")


def dan3_exact_prune_ari() -> bool:
    """Stage-3 数学的厳密枝刈り (Qwen0.6B -> Check1 -> B4 Fwd -> Check2 -> B4 Rev) を有効にするか(既定ON)。"""
    return _env_flag_on("KINGDOM_DAN3_EXACT_PRUNE", "1")


def _dan3_exact_prune_mode() -> str:
    """厳密枝刈りの不変対象モード: top1_top3(既定: R@1+R@3同時厳密不変) / top1(R@1厳密不変) / full。"""
    m = os.environ.get("KINGDOM_DAN3_EXACT_PRUNE_MODE", DAN3_EXACT_PRUNE_MODE_DEFAULT).strip().lower()
    return m if m in _DAN3_EXACT_PRUNE_VALID_MODES else DAN3_EXACT_PRUNE_MODE_DEFAULT


def _dan3_exact_prune_sym_fill_ari() -> bool:
    """Check 2 早期終了時に rr_yugo へ Fwd 対称補完(base + W*P_fwd)を使うか(既定OFF=厳密下限 mid)。"""
    return _env_flag_on("KINGDOM_DAN3_EXACT_PRUNE_SYM_FILL", "0")


def a3_template_latest_ari() -> bool:
    """A-3仮説A: 候補内同型テンプレート群の最新代表化を有効にするか(既定ON)。"""
    return _env_flag_on("KINGDOM_A3_TEMPLATE_LATEST", "1")


def a3_wadai_mirror_ari() -> bool:
    """A-3仮説B: 並存時限定の1:1単発 話題/*.md ミラー解決を有効にするか(既定ON)。"""
    return _env_flag_on("KINGDOM_A3_WADAI_MIRROR", "1")


def _dan3_listwise_boost() -> float:
    try:
        v = float(os.environ.get("KINGDOM_DAN3_BOOST", str(DAN3_LISTWISE_BOOST_DEFAULT)))
        return v if v > 0 else DAN3_LISTWISE_BOOST_DEFAULT
    except (TypeError, ValueError):
        return DAN3_LISTWISE_BOOST_DEFAULT


def _dan3_listwise_logprob_weight() -> float:
    """正逆確率平均の rr_yugo 加算重み W(既定2.5)。KINGDOM_DAN3_LOGPROB_WEIGHT で正の実数のみ上書き可。"""
    try:
        v = float(os.environ.get("KINGDOM_DAN3_LOGPROB_WEIGHT", str(DAN3_LISTWISE_LOGPROB_WEIGHT_DEFAULT)))
        return v if (v > 0 and math.isfinite(v)) else DAN3_LISTWISE_LOGPROB_WEIGHT_DEFAULT
    except (TypeError, ValueError):
        return DAN3_LISTWISE_LOGPROB_WEIGHT_DEFAULT


def _dan3_qwen06b_weight() -> float:
    """Qwen3-Reranker-0.6B 正規化確率の rr_yugo 加算重み W(既定2.5、B4 と対称)。KINGDOM_DAN3_QWEN06B_W で正の実数のみ上書き可。"""
    try:
        v = float(os.environ.get("KINGDOM_DAN3_QWEN06B_W", str(DAN3_QWEN06B_WEIGHT_DEFAULT)))
        return v if (v > 0 and math.isfinite(v)) else DAN3_QWEN06B_WEIGHT_DEFAULT
    except (TypeError, ValueError):
        return DAN3_QWEN06B_WEIGHT_DEFAULT


def _dan3_listwise_model() -> str:
    m = os.environ.get("KINGDOM_DAN3_MODEL", DAN3_LISTWISE_MODEL_DEFAULT).strip()
    return m or DAN3_LISTWISE_MODEL_DEFAULT


def _logit(raw: float) -> float:
    p = min(max(float(raw), 1e-9), 1.0 - 1e-9)
    return math.log(p / (1.0 - p))


_MACHINE_JSON_FENCE_RE = re.compile(
    r"```(?:claim-evidence|json|jsonl)\s*[\[{].*?```",
    re.DOTALL | re.IGNORECASE,
)
_INLINE_MACHINE_JSON_RE = re.compile(
    r"(?:\{[^{}]{220,}\}|\[[^\[\]]{220,}\])"
)
_KAKO_QUERY_GUARD_RE = re.compile(
    r"(?:昔|過去|以前|当時|最初|初回|経緯|歴史|変遷|いつから|前はどう|旧版|もともと|元々|当初)"
)


def _deprioritize_machine_json(text: str, drop_redundant_for_card: bool = False) -> str:
    """機械生成JSONブロック(```claim-evidence ...``` や長大JSON等)を自然言語本文より後ろへ退避し、
    自然言語の結論・実測結果を先頭窓へ優先収容する。自然言語が無い場合は原文をそのまま返す。
    """
    raw = str(text or "")
    if not raw:
        return ""
    blocks = _MACHINE_JSON_FENCE_RE.findall(raw)
    if blocks:
        prose = " ".join(_MACHINE_JSON_FENCE_RE.sub(" ", raw).split())
        if len(prose) >= 20:
            if drop_redundant_for_card and len(raw) > len(prose) * 2:
                return prose
            tail = " ".join(" ".join(b.split()) for b in blocks)
            return (prose + " " + tail).strip()
    flat = " ".join(raw.split())
    if len(flat) > 500 and ('{"' in flat or '[{"' in flat):
        inlines = _INLINE_MACHINE_JSON_RE.findall(flat)
        if inlines:
            prose2 = " ".join(_INLINE_MACHINE_JSON_RE.sub(" ", flat).split())
            if len(prose2) >= 40:
                if drop_redundant_for_card and len(flat) > len(prose2) * 2:
                    return prose2
                return (prose2 + " " + " ".join(inlines)).strip()
    return flat


def _precompute_t_idfs(query: str, idx_cache: dict | None = None,
                       hays: list[str] | None = None,
                       total_docs: int = 0) -> list[tuple[str, float]]:
    """クエリ語のコーパスIDF表を1回だけ計算する(_dan2_pure_maxp/_dan3_make_card共通・厳密等価)。"""
    terms = [normalize(t) for t in query_terms(query) if len(t) >= 2]
    if not terms:
        return []
    postings = idx_cache.get("term_postings") if isinstance(idx_cache, dict) else None
    n_docs = max(1, int(total_docs or (len(hays) if hays else 0) or 1))
    t_idfs: list[tuple[str, float]] = []
    seen_t: set[str] = set()
    for t in terms:
        if not t or t in seen_t:
            continue
        seen_t.add(t)
        if postings is not None and t in postings:
            df = len(postings[t])
        elif hays is not None:
            df = sum(1 for h in hays if t in h)
        else:
            df = 1
        if df > 0:
            idf = math.log((n_docs - df + 0.5) / (df + 0.5) + 1.0)
            t_idfs.append((t, idf))
    return t_idfs


def _idf_best_window(query: str, flat: str, idx_cache: dict | None = None,
                     hays: list[str] | None = None, total_docs: int = 0,
                     haba: int = DAN2_PURE_MAXP_WIN, hosa: int = DAN2_PURE_MAXP_STRIDE,
                     t_idfs: list[tuple[str, float]] | None = None) -> str:
    """コーパスIDF重み付きのスライディング窓から最も問いに濃い区間を切り出す。"""
    if len(flat) <= haba:
        return flat
    if t_idfs is None:
        t_idfs = _precompute_t_idfs(query, idx_cache, hays, total_docs)
    if not t_idfs:
        return flat[:haba]
    starts = list(range(0, len(flat) - haba, hosa)) + [len(flat) - haba]
    best_s, best_sc = 0, -1.0
    try:
        import brain_speed_cache as _bsc
        norm_full, offsets = _bsc.prepare_window_norm(flat, normalize)
    except Exception:
        norm_full, offsets = None, None
    if norm_full is not None:
        t_hit = [(t, idf) for t, idf in t_idfs if t in norm_full]
        if not t_hit:
            return flat[:haba]
        if offsets is None:
            for s in starts:
                nw = norm_full[s:s + haba]
                sc = sum(idf for t, idf in t_hit if t in nw)
                if sc > best_sc:
                    best_sc = sc
                    best_s = s
        else:
            for s in starts:
                nw = norm_full[offsets[s]:offsets[s + haba]]
                sc = sum(idf for t, idf in t_hit if t in nw)
                if sc > best_sc:
                    best_sc = sc
                    best_s = s
        return flat[best_s:best_s + haba]
    for s in starts:
        nw = normalize(flat[s:s + haba])
        sc = sum(idf for t, idf in t_idfs if t in nw)
        if sc > best_sc:
            best_sc = sc
            best_s = s
    return flat[best_s:best_s + haba]


def _sem_best_window(flat: str, cnum: int | None, haba: int = DAN2_PURE_MAXP_WIN) -> str | None:
    """bge-m3 最良チャンク番号(cnum >= 1)に対応する本文中心から幅 haba の窓を切り出す。"""
    if not cnum or int(cnum) < 1 or len(flat) <= haba:
        return None
    kizami, kasanari, max_kizami = 700, 120, 6
    try:
        import roundtable_imi as _imi
        kizami = int(getattr(_imi, "KIZAMI", 700))
        kasanari = int(getattr(_imi, "KASANARI", 120))
        max_kizami = int(getattr(_imi, "MAX_KIZAMI", 6))
    except Exception:
        pass
    stride = max(1, kizami - kasanari)
    c = int(cnum)
    if c <= max_kizami:
        center = (c - 1) * stride + kizami // 2
    else:
        center = max(0, len(flat) - kizami // 2)
    start = max(0, min(len(flat) - haba, center - haba // 2))
    return flat[start:start + haba]


def _call_rerank_batches(query: str, docs: list[str], batch_size: int = 48) -> dict[int, float] | None:
    """RERANK_URL へバッチ採点を依頼する(Keep-Alive＋重複文書排除)。失敗時は None(呼び出し側で fail-open)。"""
    if not docs:
        return {}
    uniq_docs: list[str] = []
    doc_to_uidx: dict[str, int] = {}
    orig_to_uidx: list[int] = []
    for d in docs:
        uid = doc_to_uidx.get(d)
        if uid is None:
            uid = len(uniq_docs)
            doc_to_uidx[d] = uid
            uniq_docs.append(d)
        orig_to_uidx.append(uid)
    uniq_scores: dict[int, float] = {}
    try:
        for off in range(0, len(uniq_docs), batch_size):
            batch = uniq_docs[off:off + batch_size]
            body = json.dumps({"query": query, "documents": batch}).encode("utf-8")
            got = _http_post_json(RERANK_URL, body, RERANK_TIMEOUT)
            for item in got.get("results", []):
                idx = int(item.get("index", -1))
                if 0 <= idx < len(batch):
                    uniq_scores[off + idx] = float(item.get("relevance_score") or 0.0)
    except Exception:
        return None
    out: dict[int, float] = {}
    for i, uid in enumerate(orig_to_uidx):
        sc = uniq_scores.get(uid)
        if sc is not None:
            out[i] = sc
    return out


def _dan2_pure_maxp(query: str, rows: list[dict],
                    best_cnum: dict[str, int] | None = None,
                    idx_cache: dict | None = None,
                    hays: list[str] | None = None,
                    total_docs: int = 0) -> list[dict]:
    """2段目採点後の上位20件長文(>800字)に対し3窓純本文MaxPをバッチ採点し logit融合(w=0.35)で再順位化する。
    失敗時は即 fail-open で入力順を無改変返却する。
    """
    if not rows or len(rows) < 2 or not dan2_pure_maxp_ari():
        return rows
    top_k = min(DAN2_PURE_MAXP_TOPK, len(rows))
    t_idfs = _precompute_t_idfs(query, idx_cache, hays, total_docs)
    docs: list[str] = []
    owners: list[int] = []
    for ci in range(top_k):
        row = rows[ci]
        flat = _deprioritize_machine_json(str(row.get("_text_zenbun") or ""), drop_redundant_for_card=False)
        if len(flat) <= DAN2_PURE_MAXP_MIN_LEN:
            continue
        atama = " / ".join(
            v for v in (str(row.get("topic") or ""), str(row.get("title") or "")) if v
        )
        key = item_key(row)
        cands_win: list[str] = [
            flat[:DAN2_PURE_MAXP_WIN],
            _idf_best_window(query, flat, idx_cache, hays, total_docs,
                             haba=DAN2_PURE_MAXP_WIN, hosa=DAN2_PURE_MAXP_STRIDE,
                             t_idfs=t_idfs),
        ]
        sem_w = _sem_best_window(flat, (best_cnum or {}).get(key), haba=DAN2_PURE_MAXP_WIN)
        if sem_w:
            cands_win.append(sem_w)
        elif len(flat) > 1400:
            cands_win.append(flat[-DAN2_PURE_MAXP_WIN:])
        seen_doc: set[str] = set()
        for w in cands_win:
            doc = " ".join((atama + " " + w).split())[:1200]
            if doc and doc not in seen_doc:
                seen_doc.add(doc)
                docs.append(doc)
                owners.append(ci)
    if not docs:
        return rows
    scores = _call_rerank_batches(query, docs)
    if not scores:
        return rows
    max_pw: dict[int, float] = {}
    for doc_idx, ci in enumerate(owners):
        sc = scores.get(doc_idx)
        if sc is not None and sc > max_pw.get(ci, -1.0):
            max_pw[ci] = sc
    if not max_pw:
        return rows
    w = DAN2_PURE_MAXP_WEIGHT
    updated: list[tuple[float, int, dict]] = []
    for pos, row in enumerate(rows):
        if pos in max_pw:
            d2_raw = float(row.get("_d2_raw") if row.get("_d2_raw") is not None else (row.get("rerank") or 0.0))
            y2 = float(row.get("rr_yugo") or 0.0)
            bonus = float(row["_d2_bonus"]) if row.get("_d2_bonus") is not None else (y2 - _logit(d2_raw))
            y_new = round((1.0 - w) * _logit(d2_raw) + w * _logit(max_pw[pos]) + bonus, 2)
            row = dict(row)
            row["rr_yugo"] = y_new
            updated.append((y_new, pos, row))
        else:
            updated.append((float(row.get("rr_yugo") or 0.0), pos, row))
    updated.sort(key=lambda t: (-t[0], t[1]))
    return [t[2] for t in updated]


def _invalidate_dan3_url_cache(model: str | None = None) -> None:
    with _DAN3_URL_CACHE_LOCK:
        if model is None:
            _DAN3_URL_CACHE.clear()
        else:
            _DAN3_URL_CACHE.pop(model, None)


def _dan3_ollama_url(model: str) -> str:
    """gpu_furiwake.ollama_url 経由で安全に Ollama URL を解決する(30秒TTLキャッシュ)。"""
    now_m = time.monotonic()
    with _DAN3_URL_CACHE_LOCK:
        hit = _DAN3_URL_CACHE.get(model)
        if hit is not None and (now_m - hit[0]) < _DAN3_URL_TTL_SEC:
            return hit[1]
    home_str = str(Path.home())
    added = False
    resolved = ""
    try:
        if home_str not in sys.path:
            sys.path.insert(0, home_str)
            added = True
        import gpu_furiwake as gf
        resolved = str(gf.ollama_url(model, "/api/generate"))
    except Exception:
        for base in ("http://127.0.0.1:11436", "http://127.0.0.1:11434"):
            try:
                urllib.request.urlopen(base + "/api/version", timeout=1.5).read(1)
                resolved = base + "/api/generate"
                break
            except Exception:
                continue
        if not resolved:
            resolved = "http://127.0.0.1:11434/api/generate"
    finally:
        if added:
            try:
                sys.path.remove(home_str)
            except ValueError:
                pass
    with _DAN3_URL_CACHE_LOCK:
        _DAN3_URL_CACHE[model] = (now_m, resolved)
    return resolved


def _dan3_make_card(row: dict, query: str, idx_cache: dict | None,
                    hays: list[str] | None, total_docs: int,
                    max_body: int = 1100,
                    t_idfs: list[tuple[str, float]] | None = None) -> str:
    """Listwise 比較用の候補カード(メタ+要約+IDF重点本文)を生成する。"""
    flat = _deprioritize_machine_json(str(row.get("_text_zenbun") or row.get("snippet") or ""), drop_redundant_for_card=True)
    atama = " / ".join(
        v for v in (str(row.get("topic") or ""), str(row.get("title") or "")) if v
    )
    summ = str(row.get("summary") or "")[:180]
    mtype = str(row.get("memory_type") or "")
    sender = str(row.get("sender") or "")
    ts = str(row.get("timestamp") or "")[:10]
    if len(flat) <= max_body:
        body = flat
    else:
        win = _idf_best_window(query, flat, idx_cache, hays, total_docs,
                               haba=750, hosa=300, t_idfs=t_idfs)
        if flat.startswith(win[:100]):
            body = flat[:max_body]
        else:
            body = flat[:320] + " … " + win
    hdr = "[" + mtype + ("/" + sender if sender else "") + ("/" + ts if ts else "") + "]"
    parts = [hdr]
    if atama:
        parts.append("題:" + atama[:60])
    if summ and not body.startswith(summ[:40]):
        parts.append("要約:" + summ)
    parts.append("本文:" + body[:max_body])
    return " ".join(parts)


def _logsumexp(vals: list[float]) -> float:
    m = max(vals)
    return m + math.log(sum(math.exp(v - m) for v in vals))


def _dan3_digit_probs_from_logprobs(got: dict, n: int) -> list[float] | None:
    """Ollama /api/generate 応答の先頭トークン top_logprobs から候補 1..n の確率分布を作る。
    同一数字の複数トークン(空白付き等)は logsumexp 集約し、出現しない候補は floor=min(min_lp-2, -18)。
    logprobs 欠落・未対応・数字トークン0件なら None(呼び出し側で従来テキストへフォールバック)。
    """
    try:
        if n < 1 or not isinstance(got, dict):
            return None
        lps = got.get("logprobs")
        if not isinstance(lps, list) or not lps or not isinstance(lps[0], dict):
            return None
        top_lps = lps[0].get("top_logprobs") or []
        if not isinstance(top_lps, list) or not top_lps:
            return None
        vals: list[float] = []
        for it in top_lps:
            try:
                v = float(it.get("logprob", -20.0))
            except (TypeError, ValueError, AttributeError):
                continue
            if math.isfinite(v):
                vals.append(v)
        min_lp = min(vals, default=-20.0)
        floor = min(min_lp - 2.0, -18.0)
        digit_lps: list[list[float]] = [[] for _ in range(n)]
        for it in top_lps:
            if not isinstance(it, dict):
                continue
            tok = str(it.get("token") or "").strip()
            if not (tok.isascii() and tok.isdigit()):
                continue
            k = int(tok)
            if not (1 <= k <= n):
                continue
            try:
                lp = float(it.get("logprob", floor))
            except (TypeError, ValueError):
                lp = floor
            if not math.isfinite(lp):
                lp = floor
            digit_lps[k - 1].append(lp)
        if not any(digit_lps):
            return None
        raw = [_logsumexp(v) if v else floor for v in digit_lps]
        m = max(raw)
        exps = [math.exp(x - m) for x in raw]
        s = sum(exps) or 1.0
        return [x / s for x in exps]
    except Exception:
        return None


def _dan3_ask_listwise(url: str, model: str, query: str, cards: list[str],
                       timeout: float = DAN3_LISTWISE_TIMEOUT_DEFAULT,
                       return_probs: bool = False):
    """正順または逆順の候補カード群から最良1件の0-based添字を返す。失敗時は -1。
    return_probs=True の時は (best_idx, probs) を返す。logprobs が得られなければ (テキスト解析添字, None)。
    """
    import urllib.request
    n = len(cards)
    lines = [
        "あなたは情報検索の再順位付け器です。",
        "以下の質問に対して、最も直接かつ具体的な根拠・回答・原因・結論・手順を含む文書を1つ選んでください。",
        "※単なる質問文・依頼文より、実際の確認結果・回答・原因・決定・手順を含む文書を優先し、本文の具体記述まで読んで判断してください。",
        f"質問: {query}",
        "",
    ]
    for i, card in enumerate(cards, 1):
        lines.append(f"[{i}] {card}")
    lines.append("")
    lines.append(f"最も適切な候補の番号（1〜{n}の半角数字1文字）のみを出力してください:")
    prompt = "\n".join(lines)
    payload: dict = {
        "model": model,
        "prompt": prompt,
        "stream": False,
        "keep_alive": "2m",
        "options": {"temperature": 0, "num_predict": 6, "num_ctx": 8192},
    }
    if return_probs:
        payload["logprobs"] = True
        payload["top_logprobs"] = DAN3_LISTWISE_TOP_LOGPROBS
        payload["options"] = {"temperature": 0, "num_predict": 1, "num_ctx": 8192}
    body = json.dumps(payload).encode("utf-8")
    try:
        got = _http_post_json(url, body, timeout)
    except Exception:
        _invalidate_dan3_url_cache(model)
        raise
    fallback_idx = -1
    resp = str(got.get("response") or "").strip()
    m = re.search(r"[1-9]", resp)
    if m:
        val = int(m.group())
        if 1 <= val <= n:
            fallback_idx = val - 1
    if not return_probs:
        return fallback_idx
    probs = _dan3_digit_probs_from_logprobs(got, n)
    if probs is None:
        return (fallback_idx, None)
    best_idx = max(range(n), key=lambda i: (probs[i], -i))
    return (best_idx, probs)


def _dan3_unpack_listwise(res, n: int) -> tuple[int, list[float] | None]:
    """_dan3_ask_listwise の戻り値(int または (idx, probs))を (idx, probs|None) に正規化する。"""
    if isinstance(res, tuple):
        idx = int(res[0]) if len(res) >= 1 and res[0] is not None else -1
        probs = res[1] if len(res) >= 2 else None
        if probs is not None:
            try:
                probs = [float(p) for p in probs]
            except (TypeError, ValueError):
                probs = None
            if probs is not None and (len(probs) != n or not all(math.isfinite(p) for p in probs)):
                probs = None
        return idx, probs
    if isinstance(res, int):
        return int(res), None
    return -1, None


def _dan3_qwen06b_doc_text(row: dict, max_chars: int = DAN3_QWEN06B_MAX_CHARS) -> str:
    """Qwen3-Reranker-0.6B 採点用の候補文書テキスト(メタ+要約+本文)を生成する。"""
    txt = str(row.get("_text_zenbun") or row.get("_rerank_bun") or row.get("snippet") or "")
    summ = str(row.get("summary") or "")
    topic = str(row.get("topic") or "")
    ts = str(row.get("timestamp") or "")[:19]
    sender = str(row.get("sender") or "")
    meta_hdr = " ".join(x for x in (f"[{ts}]" if ts else "", sender, topic) if x).strip()
    body = (summ + "\n" + txt).strip() if summ and summ not in txt[:200] else txt.strip()
    full = (meta_hdr + "\n" + body).strip() if meta_hdr else body
    return full[:max_chars]


def _dan3_qwen06b_normalize_probs(probs: list[float]) -> list[float]:
    """独立 Pointwise 確率 [0..1] を B4(総質量1.0)と同スケールのゼロベース非負単体(sum<=1.0)へ正規化する。
    - min シフト(p_i - min(p))により Top-5 全員高確率時の 6位以下との人工段差(Stage-4 干渉)をゼロ化
    - 総和が 1.0 を超える複数同時肯定時のみ総和で割り、Listwise 確率分布(和=1.0)の上書きを防ぐ
    """
    if not probs:
        return []
    lo = min(probs)
    deltas = [max(0.0, p - lo) for p in probs]
    total = sum(deltas)
    scale = total if total > 1.0 else 1.0
    return [d / scale for d in deltas]


def _call_qwen06b_rerank(query: str, docs: list[str],
                         timeout: float = DAN3_QWEN06B_TIMEOUT_DEFAULT) -> list[float] | None:
    """RERANK_QWEN06B_URL へ Top-N 候補の非負 yes 確率 [0..1] 採点を依頼する。失敗時は None(即 fail-open)。"""
    if not docs:
        return []
    import urllib.request
    url = os.environ.get("KINGDOM_RERANK_QWEN06B", RERANK_QWEN06B_URL).strip() or RERANK_QWEN06B_URL
    try:
        body = json.dumps({"query": query, "documents": docs}).encode("utf-8")
        got = _http_post_json(url, body, timeout)
        items = got.get("results")
        if not isinstance(items, list):
            return None
        probs: list[float | None] = [None] * len(docs)
        for item in items:
            if not isinstance(item, dict):
                continue
            idx = int(item.get("index", -1))
            if 0 <= idx < len(docs):
                p = float(item.get("relevance_score"))
                if not (math.isfinite(p) and 0.0 <= p <= 1.0):
                    return None
                probs[idx] = p
        if any(p is None for p in probs):
            return None
        return [float(p) for p in probs]
    except Exception:
        return None


def _dan3_exact_prune_can_stop(
    scores: list[float],
    remaining_max: float,
    mode: str = DAN3_EXACT_PRUNE_MODE_DEFAULT,
    eps: float = DAN3_EXACT_PRUNE_EPS,
) -> bool:
    """未評価の残余加算項(総質量<=1・非負・最大寄与 remaining_max)のいかなる分布でも
    目標順位が不変であるとき True を返す数学的厳密判定。
    - mode="top1": 1位が絶対不変 (s[ord[0]] - s[ord[1]] > remaining_max + eps)
    - mode="top1_top3": 1位と上位3位集合が同時に絶対不変
    - mode="full": Top-N の全順位が絶対不変
    """
    n = len(scores)
    if n < 2 or remaining_max <= 0.0:
        return True
    order = sorted(range(n), key=lambda i: (-scores[i], i))
    thresh = remaining_max + eps
    if (scores[order[0]] - scores[order[1]]) <= thresh:
        return False
    if mode == "top1":
        return True
    if mode == "top1_top3":
        if n <= 3:
            return True
        return (scores[order[2]] - scores[order[3]]) > thresh
    if mode == "full":
        return all((scores[order[k]] - scores[order[k + 1]]) > thresh for k in range(n - 1))
    return True


def _dan3_exact_prune_has_stage4_tiebreak_risk(
    top_rows: list[dict],
    scores: list[float],
    remaining_max: float,
) -> bool:
    """Stage-4 の _a3_saturated_verified_tiebreak (rr>=0.995, |rr0-rr1|<=0.005, y0-y1<=1.5)
    の境界を跨ぐ可能性がある超飽和ペア(1位未確認 candidate vs 2位一次検証証跡)を検出する。
    該当時は枝刈りを抑止して本来の完全スコア計算へ委ねる。
    """
    if len(top_rows) < 2:
        return False
    order = sorted(range(len(top_rows)), key=lambda i: (-scores[i], i))
    r0, r1 = top_rows[order[0]], top_rows[order[1]]
    rr0 = float(r0.get("rerank") or 0.0)
    rr1 = float(r1.get("rerank") or 0.0)
    if rr0 < 0.995 or rr1 < 0.995 or abs(rr0 - rr1) > 0.005:
        return False
    if not (
        str(r0.get("trust") or "") == "candidate"
        and str(r0.get("proof") or "") in ("", "未確認")
        and str(r1.get("trust") or "") in {"verified_record", "verified_report", "resolved"}
        and str(r1.get("proof") or "") in ("実測済", "王合格")
    ):
        return False
    diff = scores[order[0]] - scores[order[1]]
    return (diff - remaining_max) <= (1.5 + DAN3_EXACT_PRUNE_EPS)


def _dan3_finalize_top_rows(rows: list[dict], top_n: int, new_scores: list[float]) -> list[dict]:
    """上位 top_n 件の rr_yugo を非破壊更新して降順(タイは元添字昇順)に並べ直す。"""
    top_out = [dict(r) for r in rows[:top_n]]
    for i, r in enumerate(top_out):
        r["rr_yugo"] = round(float(new_scores[i]), 4)
    order = sorted(range(top_n), key=lambda i: (-float(top_out[i]["rr_yugo"]), i))
    return [top_out[i] for i in order] + list(rows[top_n:])


def _dan3_listwise_consensus(query: str, rows: list[dict],
                             idx_cache: dict | None = None,
                             hays: list[str] | None = None,
                             total_docs: int = 0,
                             pre_maxp_margin: float | None = None) -> list[dict]:
    """接戦時(top1_rr_yugo - top2_rr_yugo <= 1.5)のみ発火する上位5件の正順・逆順 Listwise コンセンサス。
    タイムアウト・失敗時は即 fail-open で入力順を無改変返却する。
    """
    if not rows or len(rows) < 2 or not dan3_listwise_ari():
        return rows
    # B種棄権が確実なケースではLLM呼び出しを省く（精度不変・高速化）
    if (
        rows[0].get("chikai_kiroku_nashi")
        and rows[0].get("moji_de_atatteru") is None
        and float(rows[0].get("rerank") or 0.0) < 0.005
    ):
        return rows
    rr0 = float(rows[0].get("rerank") or 0.0)
    # 汎用安全ガード1(既棄権 top1 保護): 既に公開棄権条件を満たす top1 は LLM で上書きしない
    if rr0 <= 0.0 or (rr0 < 0.005 and rows[0].get("imi") is None):
        return rows
    margin = float(rows[0].get("rr_yugo") or 0.0) - float(rows[1].get("rr_yugo") or 0.0)
    eff_margin = min(margin, pre_maxp_margin) if pre_maxp_margin is not None else margin
    rr1 = float(rows[1].get("rerank") or 0.0)
    saturated_tie = (rr0 >= 0.985 and rr1 >= 0.985)
    gate_limit = 3.5 if saturated_tie else DAN3_LISTWISE_MARGIN
    if eff_margin > gate_limit:
        return rows
    # 汎用安全ガード2(超飽和一次証跡保護): 未確認 candidate vs 機械検証済み一次証跡の超飽和タイは
    # LLM ブーストで割り込ませず Stage-4 の _a3_saturated_verified_tiebreak に委ねる
    if (
        rr0 >= 0.995 and rr1 >= 0.995 and abs(rr0 - rr1) <= 0.005 and margin <= 1.5
        and str(rows[0].get("trust") or "") == "candidate"
        and str(rows[0].get("proof") or "") in ("", "未確認")
        and str(rows[1].get("trust") or "") in {"verified_record", "verified_report", "resolved"}
        and str(rows[1].get("proof") or "") in ("実測済", "王合格")
    ):
        return rows
    top_n = min(DAN3_LISTWISE_TOPK, len(rows))
    top_rows = rows[:top_n]
    use_lp = dan3_listwise_logprob_ari()
    use_exact_prune = use_lp and dan3_exact_prune_ari()

    if use_exact_prune:
        # 第13波 Track A: Qwen0.6B -> Check 1 -> B4 Fwd -> Check 2 -> B4 Rev
        # 数学的厳密枝刈り (Branch-and-Bound Early Exit): 目標順位の精度劣化 0.000% を保証。
        w = _dan3_listwise_logprob_weight()
        prune_mode = _dan3_exact_prune_mode()
        q06_norm: list[float] | None = None
        w_q06 = 0.0
        if dan3_qwen06b_ari():
            q06_docs = [_dan3_qwen06b_doc_text(r) for r in top_rows]
            q06_raw = _call_qwen06b_rerank(query, q06_docs)
            if q06_raw is not None and len(q06_raw) == top_n:
                q06_norm = _dan3_qwen06b_normalize_probs(q06_raw)
                w_q06 = _dan3_qwen06b_weight()
        y_init = [float(r.get("rr_yugo") or 0.0) for r in top_rows]
        q_adds = [(w_q06 * q06_norm[i] if q06_norm is not None else 0.0) for i in range(top_n)]
        base_scores = [y_init[i] + q_adds[i] for i in range(top_n)]

        # Check 1 (B4 Fwd & Rev 両方スキップ):
        # saturated_tie 域(margin <= 3.5)は B4 が logprobs 非対応テキストフォールバック
        # (adaptive boost <= 3.6)へ落ちた際に逆転し得るため、Qwen0.6B 有無に関わらず
        # Check 1 を抑止して B4 Fwd を確認する(logprobs が返れば直後の Check 2 で Rev 枝刈り可)。
        allow_check1 = not saturated_tie
        if (
            allow_check1
            and _dan3_exact_prune_can_stop(base_scores, w, mode=prune_mode)
            and not _dan3_exact_prune_has_stage4_tiebreak_risk(top_rows, base_scores, w)
        ):
            return _dan3_finalize_top_rows(rows, top_n, base_scores)

        t_idfs = _precompute_t_idfs(query, idx_cache, hays, total_docs)
        cards = [_dan3_make_card(r, query, idx_cache, hays, total_docs, t_idfs=t_idfs) for r in top_rows]
        model = _dan3_listwise_model()
        url = _dan3_ollama_url(model)
        try:
            fwd_idx, fwd_probs = _dan3_unpack_listwise(
                _dan3_ask_listwise(url, model, query, cards, return_probs=True), top_n)
            if fwd_idx < 0 or fwd_idx >= top_n:
                return rows
        except Exception:
            return rows

        if fwd_probs is not None:
            half_w = 0.5 * w
            mid_scores = [base_scores[i] + half_w * fwd_probs[i] for i in range(top_n)]
            # Check 2 (B4 Rev スキップ): 残余寄与 <= W_b4 / 2 で順位が絶対不変なら即確定
            if (
                _dan3_exact_prune_can_stop(mid_scores, half_w, mode=prune_mode)
                and not _dan3_exact_prune_has_stage4_tiebreak_risk(top_rows, mid_scores, half_w)
            ):
                if _dan3_exact_prune_sym_fill_ari():
                    out_scores = [y_init[i] + (w * fwd_probs[i] + q_adds[i]) for i in range(top_n)]
                else:
                    out_scores = mid_scores
                return _dan3_finalize_top_rows(rows, top_n, out_scores)

        try:
            rev_raw, rev_probs = _dan3_unpack_listwise(
                _dan3_ask_listwise(url, model, query, list(reversed(cards)), return_probs=True), top_n)
            if rev_raw < 0 or rev_raw >= top_n:
                return rows
            rev_idx = top_n - 1 - rev_raw
        except Exception:
            return rows

        if fwd_probs is not None and rev_probs is not None:
            p_rev = list(reversed(rev_probs))
            p_avg = [0.5 * (fwd_probs[i] + p_rev[i]) for i in range(top_n)]
            final_scores = [y_init[i] + (w * p_avg[i] + q_adds[i]) for i in range(top_n)]
            return _dan3_finalize_top_rows(rows, top_n, final_scores)
    else:
        t_idfs = _precompute_t_idfs(query, idx_cache, hays, total_docs)
        cards = [_dan3_make_card(r, query, idx_cache, hays, total_docs, t_idfs=t_idfs) for r in top_rows]
        model = _dan3_listwise_model()
        url = _dan3_ollama_url(model)
        try:
            fwd_idx, fwd_probs = _dan3_unpack_listwise(
                _dan3_ask_listwise(url, model, query, cards, return_probs=use_lp), top_n)
            if fwd_idx < 0 or fwd_idx >= top_n:
                return rows
            rev_raw, rev_probs = _dan3_unpack_listwise(
                _dan3_ask_listwise(url, model, query, list(reversed(cards)), return_probs=use_lp), top_n)
            if rev_raw < 0 or rev_raw >= top_n:
                return rows
            rev_idx = top_n - 1 - rev_raw
        except Exception:
            return rows
        if fwd_probs is not None and rev_probs is not None:
            # 正逆確率平均融合: rr_yugo += W_B4 * 0.5*(P_fwd + P_rev)。入力 rows は破壊しない。
            # Qwen3-Reranker-0.6B 有効時はゼロベース単体正規化確率 W_Q06B * Q_norm を同時加算(失敗時は即 fail-open)。
            p_rev = list(reversed(rev_probs))
            p_avg = [0.5 * (fwd_probs[i] + p_rev[i]) for i in range(top_n)]
            w = _dan3_listwise_logprob_weight()
            q06_norm: list[float] | None = None
            w_q06 = 0.0
            if dan3_qwen06b_ari():
                q06_docs = [_dan3_qwen06b_doc_text(r) for r in top_rows]
                q06_raw = _call_qwen06b_rerank(query, q06_docs)
                if q06_raw is not None and len(q06_raw) == top_n:
                    q06_norm = _dan3_qwen06b_normalize_probs(q06_raw)
                    w_q06 = _dan3_qwen06b_weight()
            top_out = [dict(r) for r in rows[:top_n]]
            for i, r in enumerate(top_out):
                add = w * p_avg[i] + (w_q06 * q06_norm[i] if q06_norm is not None else 0.0)
                r["rr_yugo"] = round(float(r.get("rr_yugo") or 0.0) + add, 4)
            order = sorted(range(top_n), key=lambda i: (-float(top_out[i]["rr_yugo"]), i))
            return [top_out[i] for i in order] + list(rows[top_n:])
    winner_idx = -1
    if fwd_idx == rev_idx:
        if fwd_idx <= 0:
            return rows
        winner_idx = fwd_idx
    else:
        # 正逆で別添字を選んでも、両者が同一テンプレート骨格(日付・数値違いのみ)なら
        # LLMは正逆ともに同型群を最良と合意している。過去/特定日問いを除き最新代表を勝者とする。
        if (
            a3_template_latest_ari()
            and not _EXPLICIT_DATE_QUERY_RE.search(str(query or ""))
            and not _KAKO_QUERY_GUARD_RE.search(str(query or ""))
            and str(top_rows[fwd_idx].get("memory_type") or "") == "episodic"
            and str(top_rows[rev_idx].get("memory_type") or "") == "episodic"
            and not _row_is_suppressed(top_rows[fwd_idx])
            and not _row_is_suppressed(top_rows[rev_idx])
        ):
            raw_f = " ".join(str(top_rows[fwd_idx].get("_text_zenbun") or top_rows[fwd_idx].get("snippet") or "").split())
            raw_r = " ".join(str(top_rows[rev_idx].get("_text_zenbun") or top_rows[rev_idx].get("snippet") or "").split())
            if _are_template_siblings(top_rows[fwd_idx], raw_f, top_rows[rev_idx], raw_r):
                siblings = [fwd_idx, rev_idx]
                for k_idx in range(top_n):
                    if k_idx in siblings or _row_is_suppressed(top_rows[k_idx]):
                        continue
                    if str(top_rows[k_idx].get("memory_type") or "") != "episodic":
                        continue
                    raw_k = " ".join(str(top_rows[k_idx].get("_text_zenbun") or top_rows[k_idx].get("snippet") or "").split())
                    if _are_template_siblings(top_rows[fwd_idx], raw_f, top_rows[k_idx], raw_k):
                        siblings.append(k_idx)
                cand_win = max(siblings, key=lambda idx_: (_row_ts_key(top_rows[idx_]), -idx_))
                q_toks = [
                    tok.lower() for tok in _QUERY_SPECIFIC_TOKEN_RE.findall(str(query or ""))
                    if len(tok) >= 2
                ]
                win_low = (
                    " ".join(str(top_rows[cand_win].get("_text_zenbun") or top_rows[cand_win].get("snippet") or "").split())
                    + " " + str(top_rows[cand_win].get("summary") or "")
                ).lower()
                ref_low = (raw_f + " " + str(top_rows[fwd_idx].get("summary") or "")).lower()
                if not any((tok in ref_low) and (tok not in win_low) for tok in q_toks):
                    if cand_win > 0:
                        winner_idx = cand_win
    if winner_idx <= 0:
        return rows
    boost = _dan3_listwise_boost()
    if saturated_tie:
        gap_to_top = float(rows[0].get("rr_yugo") or 0.0) - float(rows[winner_idx].get("rr_yugo") or 0.0)
        if gap_to_top >= boost:
            boost = min(3.6, gap_to_top + 0.15)
    out = [dict(r) if i == winner_idx else r for i, r in enumerate(rows)]
    out[winner_idx]["rr_yugo"] = round(float(out[winner_idx].get("rr_yugo") or 0.0) + boost, 2)
    indexed = [(float(r.get("rr_yugo") or 0.0), i, r) for i, r in enumerate(out)]
    indexed.sort(key=lambda t: (-t[0], t[1]))
    return [t[2] for t in indexed]


# ---------------------------------------------------------------------------
# ルートA-3: 汎用規模門改善（仮説A: 同型テンプレート最新代表化 / 仮説B: 単発 話題/*.md ミラー解決）
# ---------------------------------------------------------------------------
_WADAI_ENTRY_HEADING_RE = re.compile(r"##\s*\[\d{8}\s+\d{2}:\d{2}\]")
_EXPLICIT_DATE_QUERY_RE = re.compile(
    r"(?:20\d{2}[-/年.]\d{1,2}[-/月.]\d{1,2}|\b20\d{6}\b|\d{1,2}月\d{1,2}日)"
)
_SKEL_EVENT_ID_RE = re.compile(r"\b20\d{18}-[0-9a-f]{8}\b", re.IGNORECASE)
_SKEL_DATE_RE = re.compile(r"20\d{2}[-/年.]\d{1,2}[-/月.]\d{1,2}日?|\b20\d{6}\b|\d{1,2}月\d{1,2}日")
_SKEL_TIME_RE = re.compile(r"\b\d{1,2}:\d{2}(?::\d{2})?\b|\d{1,2}時(?:\d{1,2}分)?")
_SKEL_HEX_RE = re.compile(r"\b[0-9a-f]{7,}\b", re.IGNORECASE)
_SKEL_NUM_RE = re.compile(r"\d+(?:[.,]\d+)?")
_QUERY_SPECIFIC_TOKEN_RE = re.compile(r"[A-Za-z][A-Za-z0-9_.-]{2,}|\d+(?:\.\d+)?")


def _row_is_suppressed(row: dict) -> bool:
    """訂正で無効・期限切れ・旧版として下段送りになった候補か。"""
    return bool(
        row.get("teisei_mukou")
        or row.get("ima_kigen_gire")
        or row.get("superseded")
        or row.get("kakunin_mukou")
        or str(row.get("memory_state") or "") == "replaced"
    )


def _a3_wadai_mirror_resolve(rows: list[dict], top_k: int = 20) -> list[dict]:
    """仮説B: 同一候補集合内に元 episodic と単一発言転記の 話題/*.md が並存し
    本文が包含一致する場合に限り、一次ソース(episodic)を優先(直前へ昇格)する。
    episodic 不在の 話題/*.md や複数発言の 話題/*.md は一切触らない。
    """
    if not rows or len(rows) < 2 or not a3_wadai_mirror_ari():
        return rows
    work = list(rows)
    cap = min(top_k, len(work))
    moved_keys: set[str] = set()
    idx = 0
    while idx < cap:
        m = work[idx]
        path = str(m.get("path") or "")
        if (
            str(m.get("memory_type") or "") != "procedural"
            or "話題/" not in path
            or not path.endswith(".md")
            or _row_is_suppressed(m)
        ):
            idx += 1
            continue
        md_flat = " ".join(str(m.get("_text_zenbun") or m.get("snippet") or "").split())
        headings = list(_WADAI_ENTRY_HEADING_RE.finditer(md_flat))
        if len(headings) != 1:
            idx += 1
            continue
        md_body = md_flat[headings[0].end():].strip()
        if len(md_body) < 40:
            idx += 1
            continue
        md_compact = re.sub(r"\s+", "", md_body)
        match_j = -1
        for j in range(idx + 1, cap):
            e = work[j]
            if str(e.get("memory_type") or "") != "episodic" or _row_is_suppressed(e):
                continue
            ek = item_key(e)
            if ek in moved_keys:
                continue
            ep_flat = " ".join(str(e.get("_text_zenbun") or e.get("snippet") or "").split())
            if len(ep_flat) < 40:
                continue
            ep_compact = re.sub(r"\s+", "", ep_flat)
            head_probe = ep_compact[:min(len(ep_compact), 220)]
            if head_probe not in md_compact:
                continue
            if len(ep_compact) > 260:
                tail_probe = ep_compact[-min(100, len(ep_compact)):]
                mid_probe = ep_compact[:min(len(ep_compact), 500)]
                if tail_probe not in md_compact and mid_probe not in md_compact:
                    continue
            if len(md_compact) > len(ep_compact) * 1.7 + 350:
                continue
            match_j = j
            break
        if match_j > idx:
            chosen = work.pop(match_j)
            moved_keys.add(item_key(chosen))
            work.insert(idx, chosen)
            idx += 2
        else:
            idx += 1
    return work


_SKEL_URL_RE = re.compile(r"https?://\S+", re.IGNORECASE)
_SKEL_PATH_RE = re.compile(r"\b[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+(?:/[A-Za-z0-9_.-]+)*\b")
_SKEL_ENG_ID_RE = re.compile(r"\b[A-Za-z][A-Za-z0-9_.-]{2,}\b")
_SKEL_YOUTEN_PREFIX_RE = re.compile(r"^【要点】.*?(?=【[^】]+】)", re.DOTALL)


def _strip_youten_prefix(text: str) -> str:
    """冒頭に自動付与された【要点】要約の後ろに本体見出し【...】が続く場合、本体見出し以降を返す。"""
    s = " ".join(str(text or "").split())
    if s.startswith("【要点】"):
        m = _SKEL_YOUTEN_PREFIX_RE.match(s)
        if m:
            rest = s[m.end():].strip()
            if len(rest) >= 40:
                return rest
    return s


@functools.lru_cache(maxsize=4096)
def _template_skeleton(text: str) -> str:
    """日付・時刻・ID・URL・パス・数値を正規化した本文骨格を返す。"""
    s = _strip_youten_prefix(text)
    s = _SKEL_URL_RE.sub("<URL>", s)
    s = _SKEL_EVENT_ID_RE.sub("<ID>", s)
    s = _SKEL_DATE_RE.sub("<DATE>", s)
    s = _SKEL_TIME_RE.sub("<TIME>", s)
    s = _SKEL_PATH_RE.sub("<PATH>", s)
    s = _SKEL_HEX_RE.sub("<HEX>", s)
    s = _SKEL_NUM_RE.sub("<NUM>", s)
    return " ".join(s.split())


@functools.lru_cache(maxsize=4096)
def _template_struct_skeleton(text: str) -> str:
    """日替わりの英数字アイテム識別子も<ENG>化した定型構造骨格(同一topic+sender定期レポート補助用)。"""
    s = _template_skeleton(text)
    s = _SKEL_ENG_ID_RE.sub("<ENG>", s)
    return " ".join(s.split())


def _skeleton_similarity(a: str, b: str, min_ratio: float = 0.65) -> float:
    """骨格文字列同士の高速な文字2-gram Dice類似度(長さ比ガード付き)。"""
    if not a or not b:
        return 0.0
    if a == b:
        return 1.0
    la, lb = len(a), len(b)
    if min(la, lb) < 30:
        return 0.0
    if min(la, lb) / max(la, lb) < min_ratio:
        return 0.0
    ba = {a[i:i + 2] for i in range(la - 1)}
    bb = {b[i:i + 2] for i in range(lb - 1)}
    if not ba or not bb:
        return 0.0
    return (2.0 * len(ba & bb)) / (len(ba) + len(bb))


def _are_template_siblings(row_a: dict, raw_a: str, row_b: dict, raw_b: str,
                           sim_threshold: float = 0.85) -> bool:
    """2件の episodic 候補が同型テンプレート群に属するかを汎用判定する。
    (1) 基本骨格(日付・時刻・ID・URL・パス・数値正規化)が sim_threshold 以上
    (2) 同一非空(topic, sender)の定期レポートに限り、先頭60字骨格一致(>=0.85)かつ
        英数字識別子正規化後の定型日本語骨格(>=120字)類似度 >= 0.90 なら同型と認定。
    """
    sk_a = _template_skeleton(raw_a)
    sk_b = _template_skeleton(raw_b)
    if len(sk_a) < 30 or len(sk_b) < 30:
        return False
    if _skeleton_similarity(sk_a, sk_b) >= sim_threshold:
        return True
    ta = str(row_a.get("topic") or "").strip()
    tb = str(row_b.get("topic") or "").strip()
    sa = str(row_a.get("sender") or "").strip()
    sb = str(row_b.get("sender") or "").strip()
    if ta and sa and ta == tb and sa == sb:
        st_a = _template_struct_skeleton(raw_a)
        st_b = _template_struct_skeleton(raw_b)
        if min(len(st_a), len(st_b)) >= 120:
            head_sim = _skeleton_similarity(st_a[:60], st_b[:60], min_ratio=0.75)
            if head_sim >= 0.85 and _skeleton_similarity(st_a, st_b, min_ratio=0.45) >= 0.90:
                return True
    return False


def _row_ts_key(row: dict) -> tuple[int, str]:
    raw = str(row.get("timestamp") or "").strip()
    if not raw:
        return (0, "")
    dt = _iso_datetime(raw)
    if dt is not None:
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return (1, dt.astimezone(timezone.utc).isoformat())
    return (0, raw)


def _row_trust_static_bonus(row: dict) -> float:
    """roundtable_brain.narabe_naosu の静的 trust ボーナスの写し(時代断層補正用)。"""
    trust = str(row.get("trust") or "")
    if trust in {"verified_record", "verified_report", "resolved"}:
        return 0.4
    if trust == "measured_claim":
        return 0.2
    return 0.0


def _row_is_strong_verified(row: dict) -> bool:
    trust = str(row.get("trust") or "")
    proof = str(row.get("proof") or "")
    return trust in {"verified_record", "verified_report", "resolved"} or proof == "王合格"


def _a3_template_latest_promote(rows: list[dict], query: str, ito: str,
                                top_k: int = 15, sim_threshold: float = 0.85) -> list[dict]:
    """仮説A: 上位候補内で日付・数値・ID・URL等を除いた本文骨格が高類似の
    同型テンプレート群に限り、ito != "kako" なら最新1件をその群の最上位スロットへ代表昇格する。
    非定型文書・過去問い・特定日指定クエリ・検証済み記録への未確認上書きは一切行わない。
    """
    if not rows or len(rows) < 2 or ito == "kako" or not a3_template_latest_ari():
        return rows
    if _EXPLICIT_DATE_QUERY_RE.search(str(query or "")):
        return rows
    cap = min(top_k, len(rows))
    raw_texts = [
        " ".join(str(r.get("_text_zenbun") or r.get("snippet") or "").split())
        for r in rows[:cap]
    ]
    skels = [_template_skeleton(t) for t in raw_texts]
    q_tokens = [
        tok.lower() for tok in _QUERY_SPECIFIC_TOKEN_RE.findall(str(query or ""))
        if len(tok) >= 2
    ]
    work = list(rows)
    used: set[int] = set()
    promotions: list[tuple[int, int]] = []
    for i in range(cap):
        if i in used or _row_is_suppressed(work[i]):
            continue
        if str(work[i].get("memory_type") or "") != "episodic":
            continue
        if len(skels[i]) < 30:
            continue
        group = [i]
        for j in range(i + 1, cap):
            if j in used or _row_is_suppressed(work[j]):
                continue
            if str(work[j].get("memory_type") or "") != "episodic":
                continue
            if _are_template_siblings(work[i], raw_texts[i], work[j], raw_texts[j], sim_threshold=sim_threshold):
                group.append(j)
        if len(group) < 2:
            continue
        for g_idx in group:
            used.add(g_idx)
        head_idx = group[0]
        newest_idx = max(group, key=lambda idx_: (_row_ts_key(work[idx_]), -idx_))
        if newest_idx == head_idx:
            continue
        if _row_ts_key(work[newest_idx]) <= _row_ts_key(work[head_idx]):
            continue
        # 強い検証済み一次記録(verified_record/王合格)を未確認candidateで上書きしない安全ガード
        if _row_is_strong_verified(work[head_idx]) and not _row_is_strong_verified(work[newest_idx]):
            continue
        head_y = float(work[head_idx].get("rr_yugo") or 0.0)
        new_y = float(work[newest_idx].get("rr_yugo") or 0.0)
        # 時代断層(旧 measured_claim +0.20 vs 新 candidate 0.00)の静的trust差を補正した実質差で判定
        trust_gap = max(0.0, _row_trust_static_bonus(work[head_idx]) - _row_trust_static_bonus(work[newest_idx]))
        if (head_y - new_y - trust_gap) > 3.5:
            continue
        head_low = (raw_texts[head_idx] + " " + str(work[head_idx].get("summary") or "")).lower()
        new_low = (raw_texts[newest_idx] + " " + str(work[newest_idx].get("summary") or "")).lower()
        if any((tok in head_low) and (tok not in new_low) for tok in q_tokens):
            continue
        promotions.append((head_idx, newest_idx))
    if promotions:
        pairs = [(work[h], work[n]) for h, n in promotions]
        for head_obj, new_obj in pairs:
            try:
                hi = work.index(head_obj)
                ni = work.index(new_obj)
            except ValueError:
                continue
            if ni > hi:
                work.pop(ni)
                work.insert(hi, new_obj)
    return _a3_saturated_verified_tiebreak(work, query)


def _a3_saturated_verified_tiebreak(rows: list[dict], query: str) -> list[dict]:
    """超飽和リランカー域(rerank>=0.995・確率差<=0.005・yugo差<=1.5)で上位1位が未確認candidate、
    2位が機械検証済み一次証跡(verified_record/verified_report/resolved かつ 実測済/王合格)の
    実質同率接戦に限り、一次検証証跡を先頭へ優先する(logit飽和増幅の補正)。
    """
    if not rows or len(rows) < 2 or not a3_template_latest_ari():
        return rows
    r0, r1 = rows[0], rows[1]
    if _row_is_suppressed(r0) or _row_is_suppressed(r1):
        return rows
    rr0 = float(r0.get("rerank") or 0.0)
    rr1 = float(r1.get("rerank") or 0.0)
    if rr0 < 0.995 or rr1 < 0.995 or abs(rr0 - rr1) > 0.005:
        return rows
    y0 = float(r0.get("rr_yugo") or 0.0)
    y1 = float(r1.get("rr_yugo") or 0.0)
    if (y0 - y1) > 1.5:
        return rows
    t0, p0 = str(r0.get("trust") or ""), str(r0.get("proof") or "")
    t1, p1 = str(r1.get("trust") or ""), str(r1.get("proof") or "")
    is_unverified_0 = (t0 == "candidate" and p0 in ("", "未確認"))
    is_verified_1 = (
        t1 in {"verified_record", "verified_report", "resolved"}
        and p1 in ("実測済", "王合格")
    )
    if not (is_unverified_0 and is_verified_1):
        return rows
    q_tokens = [
        tok.lower() for tok in _QUERY_SPECIFIC_TOKEN_RE.findall(str(query or ""))
        if len(tok) >= 2
    ]
    low0 = (" ".join(str(r0.get("_text_zenbun") or r0.get("snippet") or "").split()) + " " + str(r0.get("summary") or "")).lower()
    low1 = (" ".join(str(r1.get("_text_zenbun") or r1.get("snippet") or "").split()) + " " + str(r1.get("summary") or "")).lower()
    if any((tok in low0) and (tok not in low1) for tok in q_tokens):
        return rows
    out = list(rows)
    out[0], out[1] = out[1], out[0]
    return out
