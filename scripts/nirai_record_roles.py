"""Read-only record-role projection; never grants execution authority.

Existing canonical events and the brain verification inspector remain authoritative.
No new decision ledger, model call, refresh, or historical event rewrite.
"""
import argparse
import collections
import hashlib
import importlib.util
import json
import os
import sys
import threading
import time
from datetime import datetime
from pathlib import Path

# ── 台帳の写し（2026-11-01 Code・王命「くくるの作業が遅い」） ─────────────────────
# 実測: 頭脳の検索1回ごとに toru.jsonl(122MB・8万行)を全部読み、json.loads 8万回で約2.2秒。
# 使うのは kind=owari の行だけ。台帳は追記専用なので、読んだ所までの owari 行を覚え、
# 増えた行だけ読んで足す。意味は _work_evidence_zenbu（元の全部読み）と同じ:
#   ・行番号は元と同じ splitlines の数え方（写しの境目は必ず \n の直後なので割れ方が変わらない）
#   ・壊れた行・dict でない行・id の無い owari 行が1つでもあれば unavailable_or_invalid
#   ・as_of の扱い（時刻が読めない/時差が無い→unavailable、未来の行は外す）は返す直前に元と同じ順で
#   ・改行の無い最後の行は毎回読み直し、写しには入れない（書きかけを覚えない）
# 全部読み直す時: 別のファイル(dev/inode)・縮んだ・頭4KBか写しの尻4KBが変わった・最後の全部読みから10分。
# 戻す = 環境変数 NIRAI_WORK_EVIDENCE_UTSUSHI=0（元の全部読みだけを使う）
_WE_LOCK = threading.Lock()
_WE_UTSUSHI = {}
_WE_KIZAMI = 4096
_WE_ZENBU_YOMI_BYOU = 600


def _we_owari_gyou(text, mae_no_gyou):
    """元の work_evidence と同じ決まりで text を読み、(owari の行, 行の数) を返す。壊れていれば例外。"""
    rows = []
    lines = text.splitlines()
    for n, line in enumerate(lines, mae_no_gyou + 1):
        if not line.strip():
            continue
        row = json.loads(line)
        if not isinstance(row, dict):
            raise ValueError('invalid work row')
        if row.get('kind') == 'owari':
            if not isinstance(row.get('id'), str) or not row['id']:
                raise ValueError('missing work identity')
            rows.append(dict(row, _line=n))
    return rows, len(lines)


def _we_owari_yomu(path):
    """(owari の行の list, 'read') か (None, 'changed_during_read') を返す。読めない・壊れていれば例外。"""
    st = os.stat(path)
    key = (st.st_dev, st.st_ino)
    with _WE_LOCK:
        c = _WE_UTSUSHI.get(str(path))
        ima = time.monotonic()
        tsukau = bool(c and c['key'] == key and st.st_size >= c['size']
                      and ima - c['zenbu_at'] < _WE_ZENBU_YOMI_BYOU)
        with open(path, 'rb') as f:
            if tsukau:
                f.seek(0)
                if f.read(len(c['atama'])) != c['atama']:
                    tsukau = False
                else:
                    f.seek(c['size'] - len(c['shiri']))
                    if f.read(len(c['shiri'])) != c['shiri']:
                        tsukau = False
            if tsukau:
                moto_rows, moto_gyou, moto_size = c['rows'], c['gyou'], c['size']
            else:
                moto_rows, moto_gyou, moto_size = [], 0, 0
            f.seek(moto_size)
            atarashii = f.read()
        kiri = atarashii.rfind(b'\n') + 1
        sorotta, hanpa = atarashii[:kiri], atarashii[kiri:]
        rows_s, gyou_s = _we_owari_gyou(sorotta.decode('utf-8'), moto_gyou)
        rows_h, _ = _we_owari_gyou(hanpa.decode('utf-8'), moto_gyou + gyou_s)
        # 元の「読んでいる間に書き換わっていないか」と同じ役: 今読んだ所をもう一度読んで比べる
        with open(path, 'rb') as f:
            f.seek(moto_size)
            if f.read(len(atarashii)) != atarashii:
                return None, 'changed_during_read'
        if tsukau:
            atama, zenbu_at = c['atama'], c['zenbu_at']
            shiri = (c['shiri'] + sorotta)[-_WE_KIZAMI:]
        else:
            atama, zenbu_at = sorotta[:_WE_KIZAMI], ima
            shiri = sorotta[-_WE_KIZAMI:]
        rows = moto_rows + rows_s if rows_s else moto_rows
        _WE_UTSUSHI[str(path)] = {'key': key, 'size': moto_size + kiri, 'gyou': moto_gyou + gyou_s,
                                  'rows': rows, 'atama': atama, 'shiri': shiri, 'zenbu_at': zenbu_at}
        while len(_WE_UTSUSHI) > 4:
            _WE_UTSUSHI.pop(next(iter(_WE_UTSUSHI)))
        return rows + rows_h, 'read'


def _we_as_of(rows, as_of):
    """元の as_of の扱いを、owari の行へ同じ順で当てる。"""
    if as_of is None:
        return rows
    out = []
    for row in rows:
        when = datetime.fromisoformat(row.get('ts') or '')
        # Ambiguous timezone is not evidence for a historical query.
        if when.tzinfo is None or as_of.tzinfo is None:
            raise ValueError('ambiguous historical timestamp')
        if when > as_of:
            continue
        out.append(row)
    return out


def _we_musubu(rows, hits):
    linked = {}
    for index, hit in enumerate(hits):
        eid = hit.get('event_id')
        report = hit.get('report_path')
        if not eid:
            continue
        matches = []
        for row in rows:
            via = ('event_id' if row.get('daiko') == eid else
                   'report_path' if report and row.get('shoko') == report else '')
            if via:
                matches.append({'work_id': row['id'], 'state': 'reported_done',
                                'at': row.get('ts'), 'via': via,
                                'ledger_line': row['_line']})
        if matches:
            linked[index] = {'matches': matches[:3], 'total': len(matches),
                             'omitted': max(0, len(matches)-3),
                             'meaning': '過去の実施報告への参照。現在の完了・検証・許可ではない。'}
    return linked


def work_evidence(root, hits, as_of=None):
    """Exact structured references only; historical reports, not current job state."""
    if os.environ.get('NIRAI_WORK_EVIDENCE_UTSUSHI', '1') == '0':
        return _work_evidence_zenbu(root, hits, as_of)
    path = root/'roundtable/shigoto/toru.jsonl'
    try:
        rows, state = _we_owari_yomu(path)
        if rows is None:
            return {}, state
        rows = _we_as_of(rows, as_of)
    except (OSError, ValueError, TypeError, UnicodeError):
        return {}, 'unavailable_or_invalid'
    return _we_musubu(rows, hits), 'read'


def _work_evidence_zenbu(root, hits, as_of=None):
    """Exact structured references only; historical reports, not current job state."""
    path = root/'roundtable/shigoto/toru.jsonl'
    try:
        raw = path.read_bytes()
        rows = []
        for n, line in enumerate(raw.decode('utf-8').splitlines(), 1):
            if not line.strip():
                continue
            row = json.loads(line)
            if not isinstance(row, dict):
                raise ValueError('invalid work row')
            row = dict(row, _line=n)
            if row.get('kind') == 'owari':
                if not isinstance(row.get('id'), str) or not row['id']:
                    raise ValueError('missing work identity')
                if as_of is not None:
                    when = datetime.fromisoformat(row.get('ts') or '')
                    # Ambiguous timezone is not evidence for a historical query.
                    if when.tzinfo is None or as_of.tzinfo is None:
                        raise ValueError('ambiguous historical timestamp')
                    if when > as_of:
                        continue
                rows.append(row)
        # known-3(c): 台帳は追記専用。読んだ範囲（先頭 len(raw) バイト）が同じなら、解析中に
        #   後ろへ追記が入っても無害。全体を比べると追記だけで changed_during_read になり、
        #   全 hit の work_reports が落ちる（約2秒の窓と約30〜40秒に1行の追記）。書き換え・切り詰め・
        #   差し替えは先頭 len(raw) バイトが変わる/足りなくなるので従来どおり検知する。
        with path.open('rb') as f:
            if f.read(len(raw)) != raw:
                return {}, 'changed_during_read'
    except (OSError, ValueError, TypeError, UnicodeError):
        return {}, 'unavailable_or_invalid'
    linked = {}
    for index, hit in enumerate(hits):
        eid = hit.get('event_id')
        report = hit.get('report_path')
        if not eid:
            continue
        matches = []
        for row in rows:
            via = ('event_id' if row.get('daiko') == eid else
                   'report_path' if report and row.get('shoko') == report else '')
            if via:
                matches.append({'work_id': row['id'], 'state': 'reported_done',
                                'at': row.get('ts'), 'via': via,
                                'ledger_line': row['_line']})
        if matches:
            linked[index] = {'matches': matches[:3], 'total': len(matches),
                             'omitted': max(0, len(matches)-3),
                             'meaning': '過去の実施報告への参照。現在の完了・検証・許可ではない。'}
    return linked, 'read'


def enrich_results(root, hits, as_of=None, diagnostics=None):
    """Add a bounded view after retrieval; preserve scores, order and inputs."""
    if not hits:
        if diagnostics is not None:
            diagnostics['work_ledger'] = 'not_needed'
        return []
    links, state = work_evidence(root, hits, as_of) if root else ({}, 'not_checked')
    if diagnostics is not None:
        diagnostics['work_ledger'] = state
    out = []
    for index, hit in enumerate(hits):
        row = dict(hit)
        if row.get('event_id') and row.get('type'):
            role = classify(row)
            row['record_semantics'] = {
                'role': role['record_role'],
            }
            if index in links:
                row['record_semantics']['work_reports'] = links[index]
        out.append(row)
    return out


def classify(event, verified_ids=()):
    kind = event.get('type')
    role = {'proposal': 'proposal', 'instruction': 'instruction_record',
            'claim': 'work_claim', 'progress': 'execution_report',
            'complete': 'execution_report', 'blocked': 'execution_report',
            'question': 'question', 'answer': 'answer_record', 'note': 'note'}.get(kind, 'unknown')
    return {
        'event_id': event.get('event_id'), 'record_role': role,
        'declared_type': kind, 'declared_sender': event.get('sender'),
        'king_sender_recorded': event.get('sender') == '王',
        'declared_proof': event.get('proof'),
        'content_verification': ('linked_independent_evidence'
            if event.get('event_id') in verified_ids else 'not_independently_verified'),
        'identity_verification': 'not_checked',
        'execution_authority': 'not_evaluated',
        'meaning': '投稿の分類。王の決定・署名・実行許可を認定しない。',
    }


def manifest(root):
    files = sorted((root/'roundtable/events').glob('events_*.jsonl'))
    files += [root/'roundtable/brain/verifications.jsonl',
              # ★2026-09-27 ai-d6e4（王命_20260927-1833 D）: v2 の台帳も『読む物』として数える。
              #   ここに無いと、実際に読んでいる台帳が manifest に出ない＝
              #   読んでいる間に v2 が変わっても changed_during_read が気づけない。**読むだけ。**
              root/'roundtable/brain/verifications_v2.jsonl',
              root/'scripts/roundtable_brain.py']
    return {str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest()
            if p.is_file() else None for p in files}


def audit(root, event_ids=()):
    before = manifest(root)
    if not any(k.startswith('roundtable/events/') for k in before):
        raise ValueError('canonical event files unavailable')
    events, errors, seen = [], [], set()
    for rel in before:
        if not rel.startswith('roundtable/events/'):
            continue
        for n, line in enumerate((root/rel).read_text().splitlines(), 1):
            if not line.strip():
                continue
            try:
                event = json.loads(line)
                if not isinstance(event, dict) or not isinstance(event.get('event_id'), str) or not event['event_id']:
                    raise ValueError('invalid event identity')
                if event['event_id'] in seen:
                    raise ValueError('duplicate event identity')
                seen.add(event['event_id'])
                events.append(event)
            except (ValueError, TypeError):
                errors.append({'path': rel, 'line': n, 'error': 'invalid_or_duplicate_event'})
    ledger = root/'roundtable/brain/verifications.jsonl'
    if not ledger.is_file():
        errors.append({'error': 'verification_ledger_unavailable'})
    else:
        for n, line in enumerate(ledger.read_text().splitlines(), 1):
            if not line.strip():
                continue
            try:
                if not isinstance(json.loads(line), dict):
                    raise ValueError('not an object')
            except ValueError:
                errors.append({'error': 'invalid_verification_row', 'line': n})
    sys.path.insert(0, str(root/'scripts'))
    spec = importlib.util.spec_from_file_location('nirai_existing_brain', root/'scripts/roundtable_brain.py')
    brain = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(brain)
    # v1 の verification_errors を出すために inspect_verifications は残す（表示用）。
    _valid, verification_errors = brain.inspect_verifications(root, events)
    # ★2026-09-27 ai-d6e4（王命_20260927-1833 A/B/C）:
    #   前は v1 の有効行だけを verified とみなしていたので、**v2 で強検証された投稿を
    #   not_independently_verified と誤って出していた**。
    #   実測(2026-09-27 18:5x・読むだけ) = 正本 5件 に対し v1 だけでは 4件。
    #   落ちていたのは event 01790476549469711603-718b4abf（L3_reproduction / pass）。
    #   直し = 共有脳の正本と**同じ集合**を借りる（v1 の有効行 ∪ v2 の有効な強検証の最新pass）。
    #   ★v2 の判定器をここへ写さない。世界でも名の付いた悪手で、
    #     **読む側の画面が業務の規則を写し直すと正本と食い違う**
    #     （CQRS: read model は業務の規則を持たず、規則は共有して複製しない）。
    #     写すと「後から来た反証で verified が降りる」規則（v2_saishin_de_kimeru）も失う。
    verified_ids = brain.verified_event_ids(root, events)
    rows = [classify(e, verified_ids) for e in events if not event_ids or e['event_id'] in event_ids]
    after = manifest(root)
    return {'schema': 'nirai.record_roles.v1', 'read_only': True,
            'atomic_snapshot': False, 'changed_during_read': before != after,
            'source_manifest': before,
            'state': 'incomplete' if errors or verification_errors or before != after or set(event_ids)-seen else 'read',
            'errors': errors, 'verification_errors': verification_errors,
            'missing_event_ids': sorted(set(event_ids)-seen),
            'counts': dict(collections.Counter(r['record_role'] for r in rows)), 'records': rows}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path.home()/'local-ai-stack')
    parser.add_argument('--event-id', action='append', default=[])
    args = parser.parse_args()
    print(json.dumps(audit(args.root.resolve(), args.event_id), ensure_ascii=False, indent=2))
