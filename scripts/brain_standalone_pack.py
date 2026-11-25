#!/usr/bin/env python3
"""ニライBrain 頭脳単体貸し出し（スタンドアロン最小構成）書き出し・検証・バグ報告・安全更新ツール。

- GPU・ローカルAI（Ollama / Reranker / bge-m3）・外部 pip パッケージ依存ゼロ（Python 3.10+ 標準ライブラリのみ）。
- `export`: 最小 12 ファイル＋本管理ツール自身を SHA-256 マニフェスト付きで書き出す。
- `verify`: `python3 -S`（site-packages 完全無効）かつソケット全面遮断下で完全オフライン E2E を機械実証する。
- `bug-report`: 機密本文・トークン・個人パスを自動マスクした診断レポートを生成し、承認時のみ GitHub Issue 起票 URL を開く／起票する。
- `update`: GitHub Releases から最新版を取得し、全ファイル SHA-256 照合 → バックアップ → 差し替え → `verify` 自己検証 → 失敗時自動ロールバックを行う。
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import re
import shutil
import subprocess
import sys
import tempfile
import urllib.error
import urllib.parse
import urllib.request
import webbrowser
import zipfile
from datetime import datetime, timezone
from pathlib import Path

SCHEMA = "kingdom.roundtable.shared_brain.standalone_manifest.v1"
BUG_REPORT_SCHEMA = "kingdom.roundtable.shared_brain.bug_report.v1"
STANDALONE_VERSION = "0.1.0"
DEFAULT_GITHUB_REPO = "ohtake01-cell/nirai-brain"

# 隔離 python3 -S かつソケット全面遮断下で全主要コマンド完走を実証済みの最小 12 ファイル
MINIMAL_STANDALONE_FILES: tuple[str, ...] = (
    "ai_availability.py",
    "brain_bm25.py",
    "brain_cpu_agent.py",
    "brain_evolve_rank.py",
    "brain_review_triage.py",
    "brain_speed_cache.py",
    "brain_text_utils.py",
    "kingdom_read_acceleration.py",
    "nirai_record_roles.py",
    "roundtable_brain.py",
    "roundtable_event.py",
    "roundtable_media.py",
)

# 貸し出し先でも verify / bug-report / update を直接叩けるよう同梱する管理ツール自身
PACK_TOOL_FILE = "brain_standalone_pack.py"

# 任意同梱（存在すれば主題台帳・鮮度詳細・出所札などを強化できる純粋標準ライブラリ系モジュール）
OPTIONAL_STANDALONE_FILES: tuple[str, ...] = (
    "brain_no_sendo.py",
    "fact_layer.py",
    "shussho.py",
)

_SAMPLE_EVENTS: tuple[dict, ...] = (
    {
        "schema": "kingdom.roundtable.event.v2",
        "event_id": "01900000000000000101-1111aaaa",
        "timestamp": "2026-11-25T06:00:00+09:00",
        "sender": "Code",
        "to": "全員",
        "topic": "ニライBrain単体運用ガイド",
        "type": "decision",
        "trust": "verified_record",
        "proof": "実測済",
        "summary": "XR-4200モジュールは純CPUモード(cpu_agent)のみで外部GPU不要で検索できる。",
        "content": "XR-4200モジュールの標準設定は KINGDOM_BRAIN_MODE=cpu_agent、既定候補数12件、外部pip依存ゼロ。",
    },
    {
        "schema": "kingdom.roundtable.event.v2",
        "event_id": "01900000000000000102-2222bbbb",
        "timestamp": "2026-11-25T06:10:00+09:00",
        "sender": "Gemini",
        "to": "Code",
        "topic": "記憶バックアップ手順",
        "type": "note",
        "trust": "measured_claim",
        "proof": "実測済",
        "summary": "スナップショットは refresh コマンドでいつでも再生成できる。",
        "content": "roundtable/events/*.jsonl が正本であり、snapshot.json は再構築可能。",
    },
)

_README_TEMPLATE = """# ニライBrain スタンドアロン最小パッケージ（`cpu_agent` 純CPUモード）

GPU・ローカルAIサーバー・外部 pip パッケージを一切使わず、Python 3.10+ 標準ライブラリのみで
共有記憶のスナップショット生成・BM25/鮮度/訂正履歴検索・4段階 `agent_confidence` 出力を行う最小パッケージです。

## 動作要件
- **OS**: macOS / Linux / Windows (WSL2)
  - ※非 WSL のネイティブ Windows では Unix `fcntl` ファイルロックが未対応のため WSL2 を使用してください。
- **Python**: Python 3.10 以上（標準ライブラリのみ・`pip install` 不要）
- **ハードウェア**: GPU 不要・RAM 4 GB 以上（推奨 8 GB）・SSD 空き 1 GB 以上

## 3 ステップ起動手順

```bash
# 1. イベント記録から共有脳スナップショットを生成
python3 scripts/roundtable_brain.py --root . refresh

# 2. 純CPU広め拾いモード（cpu_agent）で検索（JSON出力・既定12件＋4段階 agent_confidence）
python3 scripts/roundtable_brain.py --root . search --query "検索したい内容" --mode cpu_agent

# 3. エージェント投入用コンテキスト取得（Markdown形式）
python3 scripts/roundtable_brain.py --root . context --query "検索したい内容" --mode cpu_agent --format markdown
```

環境変数 `KINGDOM_BRAIN_MODE=cpu_agent` を設定すれば、`--mode` 省略時も常に純CPUモードで動作します。

## バグ報告と安全アップデート（オプトイン）

```bash
# 不具合時の自動マスク済み診断レポート生成（勝手な外部送信は行いません）
python3 scripts/brain_standalone_pack.py bug-report --root . --summary "症状の要約"

# 診断レポートを確認後、GitHub Issue 起票画面を開く（1クリック報告）
python3 scripts/brain_standalone_pack.py bug-report --root . --summary "症状の要約" --open-github

# 最新リリースの確認・SHA-256検証＆自動ロールバック付きアップデート
python3 scripts/brain_standalone_pack.py update --root . --check-only
python3 scripts/brain_standalone_pack.py update --root .
```

## `agent_confidence.level` の読み方
- `high`: 上位候補に高い語彙・特異語一致あり（本文・日付・訂正履歴を確認して回答）
- `medium`: 上位候補群に関連記録を含む可能性が高い（上位候補を読み比べて判断）
- `low`: 語彙一致が薄い（本文精査と別キーワードでの再検索を推奨）
- `likely_absent`: 共有脳に該当記録が存在しない可能性が高い（別表現で再検索または「記録なし」判断を優先）
"""

# ---------------------------------------------------------------------------
# 自動マスク（非リーク保証）
# ---------------------------------------------------------------------------

_MASK_PATTERNS: tuple[tuple[re.Pattern[str], str], ...] = (
    # GitHub / OpenAI / Anthropic / Bearer / 汎用トークン
    (re.compile(r"\b(?:ghp|gho|ghu|ghs|ghr)_[A-Za-z0-9_]{20,}\b"), "[REDACTED_SECRET]"),
    (re.compile(r"\bgithub_pat_[A-Za-z0-9_]{20,}\b"), "[REDACTED_SECRET]"),
    (re.compile(r"\bsk-(?:proj-|ant-)?[A-Za-z0-9_\-]{16,}\b"), "[REDACTED_SECRET]"),
    (re.compile(r"(?i)\bBearer\s+[A-Za-z0-9._\-+/=]{12,}"), "Bearer [REDACTED_SECRET]"),
    (re.compile(r"(?i)\b(token|api_key|apikey|secret|password|passwd)\s*[:=]\s*[\"']?[^\s\"',;]{6,}[\"']?"), r"\1=[REDACTED_SECRET]"),
    # 機密本文 JSON フィールド
    (re.compile(r'("(?:query|content|summary|snippet|toi|seikai|body)"\s*:\s*)"(?:\\.|[^"\\])*"'), r'\1"[REDACTED_CONTENT]"'),
    # メールアドレス
    (re.compile(r"\b[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}\b"), "[REDACTED_EMAIL]"),
    # ホームディレクトリ絶対パス（ユーザー名除去）
    (re.compile(r"/Users/[^/\s\"']+"), "<HOME>"),
    (re.compile(r"/home/[^/\s\"']+"), "<HOME>"),
    (re.compile(r"[A-Za-z]:\\Users\\[^\\\s\"']+"), "<HOME>"),
    # IPv4（ループバック 127.0.0.1 以外）
    (re.compile(r"\b(?!127\.0\.0\.1\b)(?:\d{1,3}\.){3}\d{1,3}\b"), "[REDACTED_IP]"),
)


def sanitize_diagnostic_text(text: str, max_chars: int = 2000) -> str:
    """診断ログからトークン・メール・個人ホームパス・記録本文・IPを機械マスクする。"""
    s = str(text or "")
    for pat, repl in _MASK_PATTERNS:
        s = pat.sub(repl, s)
    s = "".join(ch for ch in s if ch in "\n\t" or (ord(ch) >= 32 and ord(ch) != 127))
    if len(s) > max_chars:
        s = s[:max_chars] + "\n... (truncated)"
    return s.strip()


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def export_standalone(
    src_root: Path,
    dest_root: Path,
    *,
    with_optional: bool = False,
    init_sample: bool = False,
    include_pack_tool: bool = True,
    version: str = STANDALONE_VERSION,
    repo: str = DEFAULT_GITHUB_REPO,
    overrides: dict[str, Path] | None = None,
) -> dict:
    """最小スタンドアロン構成を dest_root へ書き出し、マニフェストを返す。"""
    src_scripts = src_root / "scripts"
    dst_scripts = dest_root / "scripts"
    dst_scripts.mkdir(parents=True, exist_ok=True)
    (dest_root / "roundtable" / "events").mkdir(parents=True, exist_ok=True)
    ovr = overrides or {}

    files_to_copy: list[str] = list(MINIMAL_STANDALONE_FILES)
    optional_included: list[str] = []
    if with_optional:
        for name in OPTIONAL_STANDALONE_FILES:
            if (name in ovr and ovr[name].is_file()) or (src_scripts / name).is_file():
                files_to_copy.append(name)
                optional_included.append(name)

    copied: list[dict] = []
    total_bytes = 0
    for name in files_to_copy:
        src = ovr.get(name, src_scripts / name)
        if not src.is_file():
            raise FileNotFoundError(f"必須ファイルが見つかりません: {src}")
        dst = dst_scripts / name
        shutil.copy2(src, dst)
        size = dst.stat().st_size
        total_bytes += size
        copied.append({
            "file": f"scripts/{name}",
            "bytes": size,
            "sha256": _sha256_file(dst),
            "required": name in MINIMAL_STANDALONE_FILES,
        })

    tool_entry = None
    if include_pack_tool:
        tool_src = ovr.get(PACK_TOOL_FILE, Path(__file__).resolve())
        if not tool_src.is_file():
            tool_src = src_scripts / PACK_TOOL_FILE
        if tool_src.is_file():
            tool_dst = dst_scripts / PACK_TOOL_FILE
            shutil.copy2(tool_src, tool_dst)
            tool_entry = {
                "file": f"scripts/{PACK_TOOL_FILE}",
                "bytes": tool_dst.stat().st_size,
                "sha256": _sha256_file(tool_dst),
                "required": False,
                "role": "pack_verify_bugreport_update_tool",
            }

    if init_sample:
        sample_path = dest_root / "roundtable" / "events" / "events_sample.jsonl"
        sample_path.write_text(
            "\n".join(json.dumps(ev, ensure_ascii=False) for ev in _SAMPLE_EVENTS) + "\n",
            encoding="utf-8",
        )

    (dest_root / "README_STANDALONE.md").write_text(_README_TEMPLATE, encoding="utf-8")

    manifest = {
        "schema": SCHEMA,
        "version": str(version),
        "repository": str(repo),
        "bug_report_url": f"https://github.com/{repo}/issues/new",
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "default_mode_for_standalone": "cpu_agent",
        "env_var": "KINGDOM_BRAIN_MODE=cpu_agent",
        "external_pip_dependencies": [],
        "python_min_version": "3.10",
        "supported_platforms": ["macOS", "Linux", "Windows (WSL2)"],
        "known_limitations": [
            "非WSLのネイティブWindowsはUnix fcntlファイルロック未対応（WSL2推奨）",
            "autoモード（意味検索/リランク）には別途ローカルAIサービスが必要",
        ],
        "file_count": len(copied),
        "optional_included": optional_included,
        "total_bytes": total_bytes,
        "files": copied,
        "management_tool": tool_entry,
    }
    (dest_root / "standalone_manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    return manifest


_OFFLINE_E2E_RUNNER = r"""
import contextlib, io, json, os, socket, sys

socket_calls = []
def _strict_forbid(*a, **kw):
    socket_calls.append(("strict", repr(a)))
    raise RuntimeError(f"OFFLINE_VIOLATION: socket attempted {a}")

def _offline_refused(*a, **kw):
    socket_calls.append(("status_probe", repr(a)))
    raise ConnectionRefusedError(111, "Connection refused (simulated offline host)")

socket.create_connection = _strict_forbid
socket.socket.connect = _strict_forbid

root = sys.argv[1]
sys.path.insert(0, os.path.join(root, "scripts"))
import roundtable_brain as RB

# 1. refresh (ソケット0回)
ref = RB.refresh(RB.Path(root))
# 2. CLI search positive (--mode cpu_agent, ソケット0回)
buf_pos = io.StringIO()
with contextlib.redirect_stdout(buf_pos):
    rc_pos = RB.main(["--root", root, "search", "--query", "XR-4200 標準設定", "--mode", "cpu_agent"])
s_pos = json.loads(buf_pos.getvalue())
# 3. CLI search negative (--brain-mode cpu_agent, ソケット0回)
buf_neg = io.StringIO()
with contextlib.redirect_stdout(buf_neg):
    rc_neg = RB.main(["--root", root, "search", "--query", "QZ-88888 核融合タービン冷却手順", "--brain-mode", "cpu_agent"])
s_neg = json.loads(buf_neg.getvalue())
# 4. CLI context JSON & Markdown (ソケット0回)
buf_ctx = io.StringIO()
with contextlib.redirect_stdout(buf_ctx):
    rc_ctx = RB.main(["--root", root, "context", "--query", "XR-4200 標準設定", "--mode", "cpu_agent", "--format", "json"])
c_pos = json.loads(buf_ctx.getvalue())
buf_md = io.StringIO()
with contextlib.redirect_stdout(buf_md):
    rc_md = RB.main(["--root", root, "context", "--query", "XR-4200 標準設定", "--mode", "cpu_agent", "--format", "markdown"])
md_out = buf_md.getvalue()
# 5. audit (ソケット0回)
aud = RB.audit(RB.Path(root))

strict_violations = [x for x in socket_calls if x[0] == "strict"]

# 6. status (オフラインホスト模倣)
socket.create_connection = _offline_refused
socket.socket.connect = _offline_refused
st = RB.status_payload(RB.Path(root))

local_mods = {p.stem for p in RB.Path(root, "scripts").glob("*.py")}
third_party = [
    m for m in sorted(sys.modules)
    if "." not in m and m not in sys.stdlib_module_names and not m.startswith("_")
    and m not in local_mods
]

ok = (
    rc_pos == 0 and rc_neg == 0 and rc_ctx == 0 and rc_md == 0
    and len(strict_violations) == 0
    and len(third_party) == 0
    and s_pos.get("brain_mode") == "cpu_agent"
    and (s_pos.get("agent_confidence") or {}).get("level") in ("high", "medium")
    and not s_pos.get("abstained")
    and s_neg.get("brain_mode") == "cpu_agent"
    and (s_neg.get("agent_confidence") or {}).get("level") == "likely_absent"
    and c_pos.get("brain_mode") == "cpu_agent"
    and isinstance(c_pos.get("agent_confidence"), dict)
    and "brain_mode: `cpu_agent`" in md_out
    and "agent_confidence:" in md_out
    and aud.get("status") == "ok"
    and bool(st.get("version"))
)
print(json.dumps({
    "ok": ok,
    "strict_socket_calls": len(strict_violations),
    "third_party_modules": third_party,
    "pos_conf": (s_pos.get("agent_confidence") or {}).get("level"),
    "neg_conf": (s_neg.get("agent_confidence") or {}).get("level"),
    "ctx_conf": (c_pos.get("agent_confidence") or {}).get("level"),
    "audit_status": aud.get("status"),
    "events_indexed": (ref.get("counts") or {}).get("events", 0),
}, ensure_ascii=False))
sys.exit(0 if ok else 1)
"""


def verify_standalone(
    target_dir: Path | None = None,
    *,
    src_root: Path | None = None,
    with_optional: bool = False,
    overrides: dict[str, Path] | None = None,
) -> dict:
    """完全隔離（python3 -S・ソケット全面遮断）でスタンドアロン E2E を検証する。"""
    if target_dir is None:
        base_src = src_root or Path(__file__).resolve().parents[1]
        with tempfile.TemporaryDirectory(prefix="nirai_brain_standalone_") as td:
            td_path = Path(td)
            manifest = export_standalone(
                base_src, td_path, with_optional=with_optional, init_sample=True, overrides=overrides,
            )
            res = _run_isolated_e2e(td_path)
            res["manifest_file_count"] = manifest["file_count"]
            res["manifest_total_bytes"] = manifest["total_bytes"]
            return res
    return _run_isolated_e2e(target_dir)


def _run_isolated_e2e(pkg_dir: Path) -> dict:
    proc = subprocess.run(
        [sys.executable, "-S", "-c", _OFFLINE_E2E_RUNNER, str(pkg_dir)],
        cwd=str(pkg_dir),
        env={
            "PATH": "/usr/bin:/bin",
            "HOME": str(pkg_dir),
            "KINGDOM_BRAIN_MODE": "cpu_agent",
        },
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    if proc.returncode != 0:
        return {
            "ok": False,
            "returncode": proc.returncode,
            "stdout": proc.stdout[-1000:],
            "stderr": proc.stderr[-1000:],
        }
    return json.loads(proc.stdout.strip().splitlines()[-1])


# ---------------------------------------------------------------------------
# バグ診断レポート生成（自動マスク・オプトイン GitHub Issue 連携）
# ---------------------------------------------------------------------------

def check_manifest_integrity(pkg_root: Path) -> dict:
    """standalone_manifest.json と実ファイルの SHA-256 を照合する。"""
    mp = pkg_root / "standalone_manifest.json"
    if not mp.is_file():
        return {"has_manifest": False, "version": "unknown", "ok": False, "modified": [], "missing": []}
    manifest = json.loads(mp.read_text(encoding="utf-8"))
    entries = list(manifest.get("files") or [])
    if isinstance(manifest.get("management_tool"), dict):
        entries.append(manifest["management_tool"])
    modified: list[str] = []
    missing: list[str] = []
    for ent in entries:
        rel = str(ent.get("file") or "")
        exp = str(ent.get("sha256") or "")
        fp = pkg_root / rel
        if not fp.is_file():
            missing.append(rel)
        elif exp and _sha256_file(fp) != exp:
            modified.append(rel)
    return {
        "has_manifest": True,
        "version": str(manifest.get("version") or "unknown"),
        "repository": str(manifest.get("repository") or DEFAULT_GITHUB_REPO),
        "checked_files": len(entries),
        "ok": (len(modified) == 0 and len(missing) == 0),
        "modified": modified,
        "missing": missing,
    }


def _collect_safe_audit(pkg_root: Path) -> dict:
    """記録本文を含めない形で audit ステータスだけを収集する。"""
    scripts_dir = pkg_root / "scripts"
    if not (scripts_dir / "roundtable_brain.py").is_file():
        return {"status": "unavailable"}
    try:
        proc = subprocess.run(
            [sys.executable, "-S", str(scripts_dir / "roundtable_brain.py"), "--root", str(pkg_root), "audit"],
            cwd=str(pkg_root),
            env={"PATH": "/usr/bin:/bin", "HOME": str(pkg_root), "KINGDOM_BRAIN_MODE": "cpu_agent"},
            capture_output=True, text=True, timeout=30, check=False,
        )
        raw = proc.stdout.strip()
        if raw:
            d = json.loads(raw)
            return {
                "status": str(d.get("status") or "unknown"),
                "warning_count": len(d.get("warnings") or []),
                "error_count": len(d.get("errors") or []),
            }
        return {"status": "error", "returncode": proc.returncode}
    except Exception as exc:  # noqa: BLE001
        return {"status": "exception", "error_type": type(exc).__name__}


def build_github_issue_url(repo: str, title: str, body: str, labels: list[str]) -> str:
    params = urllib.parse.urlencode({
        "title": title[:120],
        "labels": ",".join(labels),
        "body": body[:5500],
    })
    return f"https://github.com/{repo}/issues/new?{params}"


def generate_bug_report(
    pkg_root: Path,
    *,
    summary: str = "",
    error_log_path: Path | None = None,
    error_text: str = "",
    severity: str = "auto",
    repo: str = "",
    with_verify: bool = False,
    open_github: bool = False,
    submit_github: bool = False,
) -> dict:
    """機密自動マスク済みのバグ診断レポートをローカル生成し、要求時のみ GitHub 起票へ繋ぐ。"""
    integrity = check_manifest_integrity(pkg_root)
    eff_repo = str(repo or integrity.get("repository") or DEFAULT_GITHUB_REPO).strip()
    version = str(integrity.get("version") or STANDALONE_VERSION)
    audit_info = _collect_safe_audit(pkg_root)
    verify_info = verify_standalone(pkg_root) if with_verify else None

    raw_err = str(error_text or "")
    if error_log_path and error_log_path.is_file():
        raw_err = (raw_err + "\n" + error_log_path.read_text(encoding="utf-8", errors="replace")).strip()
    masked_summary = sanitize_diagnostic_text(summary or "ニライBrain スタンドアロン不具合報告", max_chars=200)
    masked_err = sanitize_diagnostic_text(raw_err, max_chars=2000)

    eff_sev = str(severity or "auto").strip().lower()
    if eff_sev not in ("normal", "crash"):
        is_crash = (
            "Traceback" in masked_err
            or "Fatal" in masked_err
            or audit_info.get("status") not in ("ok", "unavailable")
            or (isinstance(verify_info, dict) and not verify_info.get("ok"))
        )
        eff_sev = "crash" if is_crash else "normal"

    ts_utc = datetime.now(timezone.utc)
    ts_tag = ts_utc.strftime("%Y%m%dT%H%M%SZ")
    report = {
        "schema": BUG_REPORT_SCHEMA,
        "generated_at": ts_utc.isoformat(timespec="seconds"),
        "version": version,
        "repository": eff_repo,
        "severity": eff_sev,
        "summary": masked_summary,
        "environment": {
            "os": platform.system(),
            "machine": platform.machine(),
            "python": platform.python_version(),
            "brain_mode_env": os.environ.get("KINGDOM_BRAIN_MODE", ""),
        },
        "integrity": integrity,
        "audit": audit_info,
        "verify": verify_info,
        "error_excerpt_masked": masked_err,
        "privacy_guarantee": "質問本文・記録本文・トークン・メール・個人ホームパス・非ループバックIPを自動マスク済み",
    }

    title = f"[Bug][v{version}] {masked_summary[:80]}"
    labels = ["bug", "standalone"] + (["crash"] if eff_sev == "crash" else [])
    md_lines = [
        "## 症状の要約",
        masked_summary,
        "",
        "## 環境・版情報（自動収集・個人情報非含有）",
        f"- **Version**: `v{version}`",
        f"- **Severity**: `{eff_sev}`",
        f"- **OS / Arch / Python**: `{report['environment']['os']}` / `{report['environment']['machine']}` / `Python {report['environment']['python']}`",
        f"- **Manifest Integrity**: `ok={integrity.get('ok')}` (checked={integrity.get('checked_files', 0)}, modified={integrity.get('modified')}, missing={integrity.get('missing')})",
        f"- **Audit Status**: `{audit_info.get('status')}` (warnings={audit_info.get('warning_count', 0)}, errors={audit_info.get('error_count', 0)})",
    ]
    if isinstance(verify_info, dict):
        md_lines.append(f"- **Self Verify**: `ok={verify_info.get('ok')}` (sockets={verify_info.get('strict_socket_calls')}, third_party={verify_info.get('third_party_modules')})")
    if masked_err:
        md_lines += ["", "## エラー詳細（自動マスク済み）", "```text", masked_err, "```"]
    md_lines += ["", "<!-- nirai-brain-standalone-bug-report:v1 -->"]
    md_body = "\n".join(md_lines) + "\n"

    out_dir = pkg_root / "roundtable" / "brain"
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / f"bug_report_{ts_tag}.json"
    md_path = out_dir / f"bug_report_{ts_tag}.md"
    json_path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    md_path.write_text(md_body, encoding="utf-8")

    issue_url = build_github_issue_url(eff_repo, title, md_body, labels)
    submitted_url = ""
    submit_note = "local_only"
    if submit_github:
        gh_bin = shutil.which("gh")
        if gh_bin:
            cmd = [gh_bin, "issue", "create", "--repo", eff_repo, "--title", title, "--body-file", str(md_path)]
            for lb in labels:
                cmd += ["--label", lb]
            proc = subprocess.run(cmd, capture_output=True, text=True, timeout=30, check=False)
            if proc.returncode == 0:
                submitted_url = proc.stdout.strip().splitlines()[-1]
                submit_note = "submitted_via_gh"
            else:
                submit_note = f"gh_failed_use_issue_url: {proc.stderr.strip()[:200]}"
        else:
            submit_note = "gh_not_installed_use_issue_url"
    elif open_github:
        try:
            opened = webbrowser.open(issue_url)
            submit_note = "browser_opened" if opened else "open_url_manually"
        except Exception:  # noqa: BLE001
            submit_note = "open_url_manually"

    return {
        "ok": True,
        "version": version,
        "severity": eff_sev,
        "report_json": str(json_path),
        "report_md": str(md_path),
        "github_issue_url": issue_url,
        "submitted_issue_url": submitted_url,
        "submit_status": submit_note,
    }


# ---------------------------------------------------------------------------
# 安全アップデート（GitHub Releases → SHA-256 照合 → 検証 → 失敗時ロールバック）
# ---------------------------------------------------------------------------

def _http_get_bytes(url: str, timeout: int = 30) -> bytes:
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": f"nirai-brain-standalone/{STANDALONE_VERSION}",
            "Accept": "application/vnd.github+json",
        },
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read()


def _parse_semver(v: str) -> tuple[int, ...]:
    nums = re.findall(r"\d+", str(v or "0"))
    return tuple(int(n) for n in nums) if nums else (0,)


def _extract_zip_safely(zip_path: Path, dest_dir: Path) -> None:
    dest_resolved = dest_dir.resolve()
    with zipfile.ZipFile(zip_path, "r") as zf:
        for info in zf.infolist():
            target = (dest_dir / info.filename).resolve()
            if dest_resolved != target and dest_resolved not in target.parents:
                raise RuntimeError(f"Unsafe zip entry path blocked: {info.filename}")
        zf.extractall(dest_dir)


def update_standalone(
    pkg_root: Path,
    *,
    repo: str = "",
    tag: str = "latest",
    check_only: bool = False,
    release_meta: dict | None = None,
    downloader=None,
) -> dict:
    """GitHub Releases から最新版を安全取得・SHA照合・バックアップ・検証・ロールバック付きで適用する。"""
    mp = pkg_root / "standalone_manifest.json"
    cur_manifest = json.loads(mp.read_text(encoding="utf-8")) if mp.is_file() else {}
    cur_ver = str(cur_manifest.get("version") or "0.0.0")
    eff_repo = str(repo or cur_manifest.get("repository") or DEFAULT_GITHUB_REPO).strip()
    fetch = downloader or _http_get_bytes

    if release_meta is None:
        api_url = (
            f"https://api.github.com/repos/{eff_repo}/releases/latest"
            if tag == "latest"
            else f"https://api.github.com/repos/{eff_repo}/releases/tags/{urllib.parse.quote(tag)}"
        )
        release_meta = json.loads(fetch(api_url).decode("utf-8"))

    latest_tag = str(release_meta.get("tag_name") or "").lstrip("v")
    has_update = _parse_semver(latest_tag) > _parse_semver(cur_ver)
    if check_only:
        return {
            "ok": True,
            "repository": eff_repo,
            "current_version": cur_ver,
            "latest_version": latest_tag,
            "update_available": has_update,
            "release_url": release_meta.get("html_url", ""),
        }

    assets = release_meta.get("assets") or []
    zip_asset = next((a for a in assets if str(a.get("name", "")).endswith(".zip")), None)
    man_asset = next((a for a in assets if str(a.get("name", "")) == "standalone_manifest.json"), None)
    if not zip_asset or not man_asset:
        return {"ok": False, "error": "リリース資産に .zip または standalone_manifest.json が見つかりません"}

    with tempfile.TemporaryDirectory(prefix="nirai_brain_update_") as td:
        td_path = Path(td)
        new_manifest = json.loads(fetch(man_asset["browser_download_url"]).decode("utf-8"))
        zip_path = td_path / "release.zip"
        zip_path.write_bytes(fetch(zip_asset["browser_download_url"]))
        stage = td_path / "stage"
        stage.mkdir(parents=True, exist_ok=True)
        _extract_zip_safely(zip_path, stage)

        # 全ファイル SHA-256 厳密検証（1バイトでも不一致なら本番に一切触れず中断）
        expected_entries = list(new_manifest.get("files") or [])
        if isinstance(new_manifest.get("management_tool"), dict):
            expected_entries.append(new_manifest["management_tool"])
        if not expected_entries:
            return {"ok": False, "error": "新マニフェストにファイル一覧がありません"}
        for ent in expected_entries:
            rel = str(ent.get("file") or "")
            exp_sha = str(ent.get("sha256") or "")
            staged_fp = stage / rel
            if not staged_fp.is_file():
                return {"ok": False, "error": f"zip内に必須ファイルが欠損: {rel}", "rolled_back": False}
            got_sha = _sha256_file(staged_fp)
            if not exp_sha or got_sha != exp_sha:
                return {
                    "ok": False,
                    "error": f"SHA-256 不一致のため更新を中断: {rel}",
                    "expected": exp_sha,
                    "actual": got_sha,
                    "rolled_back": False,
                }

        # バックアップ退避
        ts_tag = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S_%fZ")
        bak_dir = pkg_root / ".update_backups" / f"backup_{ts_tag}"
        bak_dir.mkdir(parents=True, exist_ok=True)
        if (pkg_root / "scripts").is_dir():
            shutil.copytree(pkg_root / "scripts", bak_dir / "scripts")
        if mp.is_file():
            shutil.copy2(mp, bak_dir / "standalone_manifest.json")

        # 差し替え適用 → オフライン自己検証 → 失敗時は自動ロールバック
        try:
            for ent in expected_entries:
                rel = str(ent.get("file") or "")
                dst = pkg_root / rel
                dst.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(stage / rel, dst)
            mp.write_text(json.dumps(new_manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

            # サンプルイベントが無い本番データディレクトリでも安全に検証するため、
            # 隔離一時ルートへ新 scripts を写して verify_standalone を実行
            with tempfile.TemporaryDirectory(prefix="nirai_brain_post_verify_") as vtd:
                vpath = Path(vtd)
                shutil.copytree(pkg_root / "scripts", vpath / "scripts")
                (vpath / "roundtable" / "events").mkdir(parents=True, exist_ok=True)
                (vpath / "roundtable" / "events" / "events_sample.jsonl").write_text(
                    "\n".join(json.dumps(ev, ensure_ascii=False) for ev in _SAMPLE_EVENTS) + "\n",
                    encoding="utf-8",
                )
                ver = verify_standalone(vpath)
            if not ver.get("ok"):
                raise RuntimeError(f"更新後自己検証に失敗: {ver}")
            return {
                "ok": True,
                "repository": eff_repo,
                "from_version": cur_ver,
                "to_version": str(new_manifest.get("version") or latest_tag),
                "backup_dir": str(bak_dir),
                "verify": ver,
                "rolled_back": False,
            }
        except Exception as exc:  # noqa: BLE001
            # ロールバック復元
            if (bak_dir / "scripts").is_dir():
                shutil.rmtree(pkg_root / "scripts", ignore_errors=True)
                shutil.copytree(bak_dir / "scripts", pkg_root / "scripts")
            if (bak_dir / "standalone_manifest.json").is_file():
                shutil.copy2(bak_dir / "standalone_manifest.json", mp)
            return {
                "ok": False,
                "error": f"{type(exc).__name__}: {exc}",
                "backup_dir": str(bak_dir),
                "rolled_back": True,
            }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="ニライBrain スタンドアロン最小パッケージ書き出し・検証・バグ報告・更新")
    parser.add_argument("--root", default=str(Path(__file__).resolve().parents[1]))
    sub = parser.add_subparsers(dest="command", required=True)

    p_exp = sub.add_parser("export", help="最小構成を指定ディレクトリへ書き出す")
    p_exp.add_argument("--dest", required=True, help="書き出し先ディレクトリ")
    p_exp.add_argument("--with-optional", action="store_true", help="任意強化モジュールも同梱する")
    p_exp.add_argument("--init-sample", action="store_true", help="サンプルイベントを初期配置する")
    p_exp.add_argument("--version", default=STANDALONE_VERSION, help="マニフェストへ記録する版番号")
    p_exp.add_argument("--repo", default=DEFAULT_GITHUB_REPO, help="バグ報告/更新先の GitHub リポジトリ")

    p_ver = sub.add_parser("verify", help="完全オフライン（python3 -S・ソケット遮断）E2E を検証する")
    p_ver.add_argument("--dir", default="", help="既存パッケージディレクトリ（省略時は一時出力して自己検証）")
    p_ver.add_argument("--with-optional", action="store_true")

    p_bug = sub.add_parser("bug-report", help="自動マスク済みの診断バグ報告を生成（承認時のみ GitHub Issue 連携）")
    p_bug.add_argument("--summary", default="", help="不具合・症状の1行要約")
    p_bug.add_argument("--error-log", default="", help="エラー出力ログのファイルパス（内容は自動マスク）")
    p_bug.add_argument("--severity", choices=("auto", "normal", "crash"), default="auto")
    p_bug.add_argument("--repo", default="", help="報告先 GitHub リポジトリ（省略時はマニフェスト既定）")
    p_bug.add_argument("--with-verify", action="store_true", help="自己検証結果も添付する")
    p_bug.add_argument("--open-github", action="store_true", help="GitHub 新規 Issue 画面をブラウザで開く")
    p_bug.add_argument("--submit-github", action="store_true", help="gh CLI で直接 Issue 起票する")

    p_upd = sub.add_parser("update", help="GitHub Releases から SHA-256 検証・自動ロールバック付きで更新する")
    p_upd.add_argument("--repo", default="", help="取得先 GitHub リポジトリ")
    p_upd.add_argument("--tag", default="latest", help="取得するリリースタグ（既定 latest）")
    p_upd.add_argument("--check-only", action="store_true", help="更新有無の確認だけ行う")

    args = parser.parse_args(argv)
    src_root = Path(args.root).expanduser().resolve()
    if args.command == "export":
        m = export_standalone(
            src_root, Path(args.dest).expanduser().resolve(),
            with_optional=args.with_optional, init_sample=args.init_sample,
            version=args.version, repo=args.repo,
        )
        print(json.dumps(m, ensure_ascii=False, indent=2))
        return 0
    if args.command == "verify":
        target = Path(args.dir).expanduser().resolve() if args.dir else None
        r = verify_standalone(target, src_root=src_root, with_optional=args.with_optional)
        print(json.dumps(r, ensure_ascii=False, indent=2))
        return 0 if r.get("ok") else 1
    if args.command == "bug-report":
        err_p = Path(args.error_log).expanduser().resolve() if args.error_log else None
        r = generate_bug_report(
            src_root,
            summary=args.summary,
            error_log_path=err_p,
            severity=args.severity,
            repo=args.repo,
            with_verify=args.with_verify,
            open_github=args.open_github,
            submit_github=args.submit_github,
        )
        print(json.dumps(r, ensure_ascii=False, indent=2))
        return 0 if r.get("ok") else 1
    if args.command == "update":
        r = update_standalone(src_root, repo=args.repo, tag=args.tag, check_only=args.check_only)
        print(json.dumps(r, ensure_ascii=False, indent=2))
        return 0 if r.get("ok") else 1
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
