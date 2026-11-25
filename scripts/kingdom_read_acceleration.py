"""Content-verified read projections; no execution, acknowledgement or TTL.

Disk hits verify archived source bytes; only validated parsing is reused.
Append reuse checks the full frozen prefix, independent of file time and row count.
Settled same-stamp files reuse their SHA (sha_remembered).
The cached values are private; callers receive fresh aggregate dictionaries.
"""
import copy
import hashlib
import io
import json
import marshal
import os
import secrets
import stat
import sys
import threading
import time
from collections import OrderedDict
from contextlib import contextmanager, suppress
from pathlib import Path
from types import MappingProxyType

_LOCK = threading.RLock()
_RUNS = OrderedDict()
_INDEX = OrderedDict()
_EVENTS = OrderedDict()
_EVENT_META = OrderedDict()
_EVENT_META_SORTED = OrderedDict()
_EVENTS_RO = OrderedDict()
_EVENTS_RO_SORTED = OrderedDict()
_EVENTS_INVALID = OrderedDict()
_SHA = OrderedDict()
# 2026-11-08 ai-c0b4: git racy-git の型・10分で読み直す
SHA_RECHECK_SECONDS = 600.0
RACY_MARGIN_NS = 2_000_000_000
MAX_PARTS = 256
MAX_EVENT_FILES = 1024
MAX_INDEX_ENTRIES = 8
_STATS = {'run_rows_parsed': 0, 'run_hits': 0, 'run_appends': 0,
          'run_rebuilds': 0, 'index_parses': 0, 'index_hits': 0,
          'sha_hits': 0, 'sha_reads': 0,
          'run_disk_hits': 0, 'run_disk_writes': 0}


def _stamp(s):
    return (s.st_dev, s.st_ino, s.st_size, s.st_mtime_ns, s.st_ctime_ns)


def _stat(path):
    return _stamp(path.stat())


def _on():
    return os.environ.get('KINGDOM_READ_SHIRUSHI', '1') != '0'


def sha_remembered(path, stamp):
    with _LOCK:
        hit = _SHA.get(os.path.abspath(path))
        if _on() and hit and hit[0] == stamp and 0 <= time.time() - hit[2] < SHA_RECHECK_SECONDS:
            _STATS['sha_hits'] += 1
            return hit[1]
    return None


def sha_remember(path, stamp, sha, started_ns):
    """Remember only if mtime and ctime predate the read start by RACY_MARGIN_NS."""
    key = os.path.abspath(path)
    with _LOCK:
        _STATS['sha_reads'] += 1
        _SHA.pop(key, None)
        if _on() and max(stamp[3], stamp[4]) < started_ns - RACY_MARGIN_NS:
            _SHA[key] = (stamp, sha, time.time())
            while len(_SHA) > 8192:
                _SHA.popitem(last=False)


def sha_file_remembered(path):
    path = Path(path)
    stamp = _stat(path)
    sha = sha_remembered(path, stamp)
    if sha is None:
        started = time.time_ns()
        raw = path.read_bytes()
        sha = hashlib.sha256(raw).hexdigest()
        if _stat(path) == stamp and len(raw) == stamp[2]:
            sha_remember(path, stamp, sha, started)
    return sha


def _sha_raw(path, hit_sha):
    """(sha, raw, stamp); raw is None when the remembered sha equals hit_sha."""
    stamp = _stat(path)
    sha = sha_remembered(path, stamp)
    if sha is not None and sha == hit_sha:
        return sha, None, stamp
    started = time.time_ns()
    raw, stamp = _read(path)
    sha = hashlib.sha256(raw).hexdigest()
    sha_remember(path, stamp, sha, started)
    return sha, raw, stamp


def _read(path):
    before = _stat(path)
    raw = path.read_bytes()
    after = _stat(path)
    if before != after or len(raw) != after[2]:
        raise ValueError('changed_during_read')
    return raw, after


def _empty():
    return {field: {} for field in _RUN_GROUPS}


def _apply(out, row):
    # SKIPPED is still parsed, but has never superseded a real run.
    if row['state'] == 'SKIPPED':
        return
    sid, rid = row['spec_id'], row['run_id']
    row = MappingProxyType({k: row.get(k) for k in
        _RUN_FIELDS})
    out['latest'][rid] = row
    out['last'][sid] = row
    if row['state'] == 'CLOSED':
        out['success'][sid] = row


_RUN_CACHE_ENV = 'KINGDOM_READ_RUN_CACHE_DIR'
_RUN_CACHE_VERSION = 1
_RUN_CACHE_HEADER_LIMIT = 1024
_RUN_CACHE_PAYLOAD_LIMIT = 128 * 1024 * 1024
_RUN_FIELDS = ('spec_id', 'run_id', 'state', 'at', 'trace_id', 'source_event_id')
_RUN_GROUPS = ('latest', 'last', 'success')


@contextmanager
def _run_disk_cache(base):
    """Opt in with an existing, private, writable direct child of /tmp."""
    cache = None
    fd = None
    configured = os.environ.get(_RUN_CACHE_ENV)
    if configured and _on():
        try:
            path = Path(configured)
            root = Path('/tmp').resolve()
            source = base.resolve()
            directory = root / path.name
            if (path.is_absolute() and path.parent == Path('/tmp') and path.name != '..'
                    and not directory.is_relative_to(source) and not source.is_relative_to(directory)):
                fd = os.open(directory, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
                info = os.fstat(fd)
                if (info.st_uid == os.geteuid() and stat.S_IMODE(info.st_mode) == 0o700
                        and not os.fstatvfs(fd).f_flag & os.ST_RDONLY):
                    from workspec_runs import validate_row
                    # Bind to the executing validator and JSON integer policy.
                    # Bump the format version if the projection or encoding changes.
                    validator = hashlib.sha256(marshal.dumps(validate_row.__code__)).hexdigest()
                    version = [_RUN_CACHE_VERSION, sys.implementation.name,
                               list(sys.version_info), sys.get_int_max_str_digits(), validator]
                    cache = fd, version
        except Exception:
            # This entire block is optional cache setup. Let the source path
            # report its own errors through the same parser as before.
            pass
    try:
        yield cache
    finally:
        if fd is not None:
            with suppress(OSError):
                os.close(fd)


def _source_sha(path, stamp):
    """Hash even on disk hits and reject a replacement or write during the read."""
    fd = os.open(path, os.O_RDONLY | os.O_NONBLOCK)
    try:
        before = os.fstat(fd)
        if not stat.S_ISREG(before.st_mode) or _stamp(before) != stamp:
            return None
        with os.fdopen(fd, 'rb', closefd=False) as source:
            sha = hashlib.file_digest(source, 'sha256').hexdigest()
        if _stamp(os.fstat(fd)) == stamp and _stat(path) == stamp:
            return sha
    finally:
        os.close(fd)
    return None


def _read_run_entry(entry, info, path, stamp, version):
    header = entry.readline(_RUN_CACHE_HEADER_LIMIT)
    if not header.endswith(b'\n'):
        return None
    stored_version, stored_stamp, sha, count, checksum = json.loads(header)
    size = info.st_size - len(header)
    if (stored_version != version or stored_stamp != list(stamp)
            or type(count) is not int or not 0 <= count <= stamp[2]
            or not 0 < size <= _RUN_CACHE_PAYLOAD_LIMIT):
        return None
    observed_sha = _source_sha(path, stamp)
    if observed_sha is None or observed_sha != sha:
        return None
    payload = entry.read(size + 1)
    if len(payload) == size and hashlib.sha256(payload).hexdigest() == checksum:
        return sha, count, payload
    return None


def _load_run_part(cache, name, path, key, stamp, started):
    directory_fd, version = cache
    try:
        fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory_fd)
        try:
            info = os.fstat(fd)
            if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid()
                    or stat.S_IMODE(info.st_mode) != 0o600):
                return None
            with os.fdopen(fd, 'rb', closefd=False) as entry:
                cached = _read_run_entry(entry, info, path, stamp, version)
        finally:
            os.close(fd)
        if cached is None:
            return None
        sha, count, payload = cached
        rows, groups = json.loads(payload)
        shared = [MappingProxyType(dict(zip(_RUN_FIELDS, row, strict=True))) for row in rows]
        summary = {
            field: {key_value: shared[index] for key_value, index in pairs}
            for field, pairs in zip(_RUN_GROUPS, groups, strict=True)
        }
    except Exception:
        # Treat an inaccessible or damaged cache as a miss. Only the source
        # parser may decide whether the run ledger itself is invalid.
        return None
    sha_remember(path, stamp, sha, started)
    _STATS['run_hits'] += 1
    _STATS['run_disk_hits'] += 1
    _remember_run_part(key, sha, stamp[2], summary, count)
    return summary


def _save_run_part(cache, name, key, stamp):
    fd, version = cache
    temp = None
    try:
        hit = _RUNS[key]
        # Preserve insertion order, the exact JSON key types, and row sharing
        # among the three maps. No executable serialization is read from disk.
        rows = []
        by_identity = {}
        groups = []
        for field in _RUN_GROUPS:
            pairs = []
            for key_value, row in hit['summary'][field].items():
                identity = id(row)
                index = by_identity.get(identity)
                if index is None:
                    index = len(rows)
                    by_identity[identity] = index
                    rows.append(tuple(row[field] for field in _RUN_FIELDS))
                pairs.append((key_value, index))
            groups.append(pairs)
        payload = json.dumps([rows, groups], separators=(',', ':')).encode('ascii')
        if len(payload) > _RUN_CACHE_PAYLOAD_LIMIT:
            return
        checksum = hashlib.sha256(payload).hexdigest()
        header = json.dumps([version, stamp, hit['sha'], hit['count'], checksum]).encode('ascii')
        if len(header) + 1 > _RUN_CACHE_HEADER_LIMIT:
            return
        temporary_name = f'.{name}.{secrets.token_hex(12)}.tmp'
        entry_fd = os.open(temporary_name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                           0o600, dir_fd=fd)
        temp = temporary_name
        try:
            with os.fdopen(entry_fd, 'wb', closefd=False) as entry:
                entry.write(header + b'\n')
                entry.write(payload)
        finally:
            os.close(entry_fd)
        if _stat(Path(key)) == stamp:
            os.replace(temp, name, src_dir_fd=fd, dst_dir_fd=fd)
            temp = None
            _STATS['run_disk_writes'] += 1
    except Exception:
        # EROFS, permissions, quota, disk full and serialization failures are
        # transparent to the already validated in-memory projection.
        pass
    finally:
        if temp is not None:
            with suppress(OSError):
                os.unlink(temp, dir_fd=fd)


def _remember_run_part(key, sha, size, summary, count):
    _RUNS[key] = {'sha': sha, 'size': size, 'summary': summary, 'count': count}
    _RUNS.move_to_end(key)
    while len(_RUNS) > MAX_PARTS:
        _RUNS.popitem(last=False)


def _part(path, raw, sha=None):
    if raw and not raw.endswith(b'\n'):
        raise ValueError('incomplete runs')
    key = str(path.resolve())
    sha = sha or hashlib.sha256(raw).hexdigest()
    hit = _RUNS.get(key)
    if hit and hit['sha'] == sha and hit['size'] == len(raw):
        _STATS['run_hits'] += 1
        _RUNS.move_to_end(key)
        return hit['summary']
    offset = 0
    count = 0
    out = _empty()
    if (hit and len(raw) > hit['size']
            and hashlib.sha256(memoryview(raw)[:hit['size']]).hexdigest() == hit['sha']):
        offset = hit['size']
        count = hit['count']
        out = {k: dict(v) for k, v in hit['summary'].items()}
        _STATS['run_appends'] += 1
    else:
        _STATS['run_rebuilds'] += 1
    from workspec_runs import validate_row
    for line in raw[offset:].splitlines():
        row = validate_row(json.loads(line))
        # Validate before dropping SKIPPED or retaining only display fields.
        _apply(out, row)
        count += 1
        _STATS['run_rows_parsed'] += 1
    _remember_run_part(key, sha, len(raw), out, count)
    return out


def _run_part_projection(path, cache):
    key = str(path.resolve())
    hit = _RUNS.get(key)
    if cache is not None:
        # One slot per source path. A stale slot is replaced after validation.
        name = hashlib.sha256(os.fsencode(key)).hexdigest() + '.run-part-v2'
        if hit is None:
            stamp = _stat(path)
            part = _load_run_part(cache, name, path, key, stamp, time.time_ns())
            if part is not None:
                return part, stamp

    sha, raw, stamp = _sha_raw(path, hit and hit['sha'])
    part = hit['summary'] if raw is None else _part(path, raw, sha)
    if cache is not None and hit is None:
        # Returned mapping proxies can contain nested JSON. A process-local
        # summary already exposed to callers must never be published to disk.
        _save_run_part(cache, name, key, stamp)
    return part, stamp


def run_projection(base):
    """Reduce all current parts in order; a read never gates run_once/emit."""
    base = Path(base)
    with _LOCK, _run_disk_cache(base) as cache:  # Single flight inside one process.
        paths = sorted(base.glob('runs-[0-9]*.jsonl'), key=lambda p: p.name)
        active = base / 'runs.jsonl'
        if active.exists():
            paths.append(active)
        observed = []
        out = _empty()
        for path in paths:
            part, stamp = _run_part_projection(path, cache if path != active else None)
            observed.append((path, stamp))
            for field in out:
                out[field].update(part[field])
        current = sorted(base.glob('runs-[0-9]*.jsonl'), key=lambda p: p.name)
        if active.exists():
            current.append(active)
        if current != paths or any(_stat(p) != s for p, s in observed):
            raise ValueError('changed_during_read')
        # The summaries retain only fields used by the view. Raw execution
        # and duplicate suppression continue to read/validate full history.
        return out


def index_entry(path, relative):
    """Validate the entire index schema and count exact matches, SHA keyed."""
    with _LOCK:
        key = (str(Path(path).resolve()), relative)
        hit = _INDEX.get(key)
        sha, raw, stamp = _sha_raw(Path(path), hit and hit[0])
        if hit and hit[0] == sha:
            _STATS['index_hits'] += 1
            _INDEX.move_to_end(key)
            return dict(hit[1])
        value = json.loads(raw)
        rows = value['records']
        if (not isinstance(rows, list)
                or any(not isinstance(r, dict) or not isinstance(r.get('path'), str) for r in rows)):
            raise ValueError('invalid records schema')
        matches = [r for r in rows if r['path'] == relative]
        answer = {'index_generated': value.get('generated'), 'matches': len(matches),
                  'indexed_sha256': matches[0].get('sha256') if len(matches) == 1 else None}
        if _stat(Path(path)) != stamp:
            raise ValueError('changed_during_read')
        _INDEX[key] = (sha, answer)
        _INDEX.move_to_end(key)
        while len(_INDEX) > MAX_INDEX_ENTRIES:
            _INDEX.popitem(last=False)
        _STATS['index_parses'] += 1
        return dict(answer)


def _json_copy(o):
    """json.loads で作った物（dict/list と変わらない値）だけの写し。copy.deepcopy と同じ中身を作る。

    2026-11-01 Code（王命「くくるの作業が遅い」）: 頭脳の検索1回ごとに円卓の記録2万8千件を
    copy.deepcopy して約1.6秒かかっていた。json.loads の結果は中で物を共有せず、str/int/float/
    bool/None は変わらない値なので、dict と list だけを作り直せば deepcopy と同じ物になる。
    それ以外の型が混じっていたら、その部分だけ copy.deepcopy に任せる（意味を変えない）。
    """
    t = type(o)
    if t is dict:
        return {k: _json_copy(v) for k, v in o.items()}
    if t is list:
        return [_json_copy(v) for v in o]
    if t is str or t is int or t is float or t is bool or o is None:
        return o
    return copy.deepcopy(o)


def brain_events(root):
    """Match shared-brain read_jsonl semantics, returning isolated event objects."""
    root = Path(root)
    paths = sorted((root/'roundtable/events').glob('events_*.jsonl'))
    rows = []
    with _LOCK:
        for path in paths:
            key = str(path.resolve())
            hit = _EVENTS.get(key)
            sha, raw, _ = _sha_raw(path, hit and hit[0])
            if not hit or hit[0] != sha:
                decoded = []
                source = str(path.relative_to(root))
                # Universal newline handling, without splitting literal Unicode
                # line separators inside valid JSON strings.
                for no, line in enumerate(io.StringIO(raw.decode('utf-8', errors='replace'), newline=None), 1):
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        event = json.loads(line)
                    except json.JSONDecodeError:
                        continue  # load_events has always excluded invalid JSON.
                    if isinstance(event, dict) and not event.get('_invalid'):
                        event = dict(event)
                        event['_source'] = source
                        decoded.append(event)
                _EVENTS[key] = (sha, decoded)
            _EVENTS.move_to_end(key)
            rows.extend([_json_copy(e) for e in _EVENTS[key][1]])
        while len(_EVENTS) > MAX_EVENT_FILES:
            _EVENTS.popitem(last=False)
    rows.sort(key=lambda item: (str(item.get('timestamp') or ''), str(item.get('event_id') or '')))
    return rows


def brain_events_meta_with_sig(root):
    """Return (sig, rows) of immutable MappingProxyType({'event_id', 'timestamp'}) without deep-copying full events."""
    root = Path(root)
    paths = sorted((root/'roundtable/events').glob('events_*.jsonl'))
    sig_parts = []
    per_file = []
    with _LOCK:
        for path in paths:
            key = str(path.resolve())
            hit_ev = _EVENTS.get(key)
            hit_meta = _EVENT_META.get(key)
            hit_sha = (hit_meta[0] if hit_meta else (hit_ev[0] if hit_ev else None))
            sha, raw, _ = _sha_raw(path, hit_sha)
            if hit_meta and hit_meta[0] == sha:
                meta_rows = hit_meta[1]
            elif hit_ev and hit_ev[0] == sha:
                meta_rows = tuple(
                    MappingProxyType({
                        'event_id': str(e.get('event_id') or ''),
                        'timestamp': str(e.get('timestamp') or ''),
                    })
                    for e in hit_ev[1]
                )
                _EVENT_META[key] = (sha, meta_rows)
            else:
                decoded = []
                meta_list = []
                source = str(path.relative_to(root))
                for no, line in enumerate(io.StringIO(raw.decode('utf-8', errors='replace'), newline=None), 1):
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        event = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if isinstance(event, dict) and not event.get('_invalid'):
                        event = dict(event)
                        event['_source'] = source
                        decoded.append(event)
                        meta_list.append(MappingProxyType({
                            'event_id': str(event.get('event_id') or ''),
                            'timestamp': str(event.get('timestamp') or ''),
                        }))
                meta_rows = tuple(meta_list)
                _EVENTS[key] = (sha, decoded)
                _EVENT_META[key] = (sha, meta_rows)
            if key in _EVENTS:
                _EVENTS.move_to_end(key)
            _EVENT_META.move_to_end(key)
            sig_parts.append((key, sha))
            per_file.append(meta_rows)
        while len(_EVENTS) > MAX_EVENT_FILES:
            _EVENTS.popitem(last=False)
        while len(_EVENT_META) > MAX_EVENT_FILES:
            _EVENT_META.popitem(last=False)
        sig = tuple(sig_parts)
        root_key = str(root.resolve())
        cached = _EVENT_META_SORTED.get(root_key)
        if cached and cached[0] == sig:
            _EVENT_META_SORTED.move_to_end(root_key)
            return sig, list(cached[1])
        merged = []
        for chunk in per_file:
            merged.extend(chunk)
        merged.sort(key=lambda item: (item['timestamp'], item['event_id']))
        frozen = tuple(merged)
        _EVENT_META_SORTED[root_key] = (sig, frozen)
        _EVENT_META_SORTED.move_to_end(root_key)
        while len(_EVENT_META_SORTED) > 16:
            _EVENT_META_SORTED.popitem(last=False)
        return sig, list(frozen)


def brain_events_meta(root):
    """Lightweight read-only event metadata (event_id, timestamp) matching brain_events semantics."""
    _, rows = brain_events_meta_with_sig(root)
    return rows


class _ReadOnlyDict(dict):
    """dict互換の不変ビュー。isinstance(x, dict) と json.dumps を保ちつつキャッシュ汚染を防ぐ。"""

    def _ro(self, *args, **kwargs):
        raise TypeError("read-only event dict cannot be mutated")

    __setitem__ = _ro
    __delitem__ = _ro
    clear = _ro
    pop = _ro
    popitem = _ro
    setdefault = _ro
    update = _ro

    def __ior__(self, other):
        raise TypeError("read-only event dict cannot be mutated")


class _ReadOnlyList(list):
    """list互換の不変ビュー。isinstance(x, list) と json.dumps を保ちつつキャッシュ汚染を防ぐ。"""

    def _ro(self, *args, **kwargs):
        raise TypeError("read-only event list cannot be mutated")

    __setitem__ = _ro
    __delitem__ = _ro
    append = _ro
    clear = _ro
    extend = _ro
    insert = _ro
    pop = _ro
    remove = _ro
    reverse = _ro
    sort = _ro
    __iadd__ = _ro
    __imul__ = _ro


def _freeze_readonly(o):
    t = type(o)
    if t is dict:
        return _ReadOnlyDict((k, _freeze_readonly(v)) for k, v in o.items())
    if t is list:
        return _ReadOnlyList(_freeze_readonly(v) for v in o)
    return o


def brain_events_readonly_with_sig(root):
    """Return (sig, rows) of immutable dict/list-compatible full events without per-call deep copies."""
    root = Path(root)
    paths = sorted((root/'roundtable/events').glob('events_*.jsonl'))
    sig_parts = []
    per_file = []
    with _LOCK:
        for path in paths:
            key = str(path.resolve())
            hit_ev = _EVENTS.get(key)
            hit_ro = _EVENTS_RO.get(key)
            hit_sha = (hit_ro[0] if hit_ro else (hit_ev[0] if hit_ev else None))
            sha, raw, _ = _sha_raw(path, hit_sha)
            if hit_ro and hit_ro[0] == sha:
                ro_rows = hit_ro[1]
            elif hit_ev and hit_ev[0] == sha:
                ro_rows = tuple(_freeze_readonly(e) for e in hit_ev[1])
                _EVENTS_RO[key] = (sha, ro_rows)
            else:
                decoded = []
                ro_list = []
                bad_list = []
                source = str(path.relative_to(root))
                for no, line in enumerate(io.StringIO(raw.decode('utf-8', errors='replace'), newline=None), 1):
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        event = json.loads(line)
                    except json.JSONDecodeError:
                        bad_list.append({
                            'file': source, 'line': no,
                            'sha256': hashlib.sha256(line.encode('utf-8')).hexdigest(),
                        })
                        continue
                    if isinstance(event, dict) and not event.get('_invalid'):
                        event = dict(event)
                        event['_source'] = source
                        decoded.append(event)
                        ro_list.append(_freeze_readonly(event))
                    elif isinstance(event, dict) and event.get('_invalid'):
                        bad_list.append({
                            'file': source, 'line': event.get('_line', no),
                            'sha256': event.get('_raw_sha256') or hashlib.sha256(line.encode('utf-8')).hexdigest(),
                        })
                ro_rows = tuple(ro_list)
                _EVENTS[key] = (sha, decoded)
                _EVENTS_RO[key] = (sha, ro_rows)
                _EVENTS_INVALID[key] = (sha, tuple(bad_list))
            if key in _EVENTS:
                _EVENTS.move_to_end(key)
            _EVENTS_RO.move_to_end(key)
            sig_parts.append((key, sha))
            per_file.append(ro_rows)
        while len(_EVENTS) > MAX_EVENT_FILES:
            _EVENTS.popitem(last=False)
        while len(_EVENTS_RO) > MAX_EVENT_FILES:
            _EVENTS_RO.popitem(last=False)
        sig = tuple(sig_parts)
        root_key = str(root.resolve())
        cached = _EVENTS_RO_SORTED.get(root_key)
        if cached and cached[0] == sig:
            _EVENTS_RO_SORTED.move_to_end(root_key)
            return sig, list(cached[1])
        merged = []
        for chunk in per_file:
            merged.extend(chunk)
        merged.sort(key=lambda item: (str(item.get('timestamp') or ''), str(item.get('event_id') or '')))
        frozen = tuple(merged)
        _EVENTS_RO_SORTED[root_key] = (sig, frozen)
        _EVENTS_RO_SORTED.move_to_end(root_key)
        while len(_EVENTS_RO_SORTED) > 16:
            _EVENTS_RO_SORTED.popitem(last=False)
        return sig, list(frozen)


def brain_events_readonly(root):
    """Immutable dict/list-compatible full events matching brain_events semantics without deep copy."""
    _, rows = brain_events_readonly_with_sig(root)
    return rows


def brain_events_invalid(root):
    """Return broken/invalid JSONL rows in roundtable/events/events_*.jsonl with per-file SHA cache."""
    root = Path(root)
    paths = sorted((root/'roundtable/events').glob('events_*.jsonl'))
    out = []
    with _LOCK:
        for path in paths:
            key = str(path.resolve())
            hit = _EVENTS_INVALID.get(key)
            sha, raw, _ = _sha_raw(path, hit and hit[0])
            if hit and hit[0] == sha:
                bad_rows = hit[1]
            else:
                bad_list = []
                rel = str(path.relative_to(root))
                for no, line in enumerate(io.StringIO(raw.decode('utf-8', errors='replace'), newline=None), 1):
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        val = json.loads(line)
                    except json.JSONDecodeError:
                        bad_list.append({
                            'file': rel, 'line': no,
                            'sha256': hashlib.sha256(line.encode('utf-8')).hexdigest(),
                        })
                        continue
                    if isinstance(val, dict) and val.get('_invalid'):
                        bad_list.append({
                            'file': rel, 'line': val.get('_line', no),
                            'sha256': val.get('_raw_sha256') or hashlib.sha256(line.encode('utf-8')).hexdigest(),
                        })
                bad_rows = tuple(bad_list)
                _EVENTS_INVALID[key] = (sha, bad_rows)
            _EVENTS_INVALID.move_to_end(key)
            for r in bad_rows:
                out.append(dict(r))
        while len(_EVENTS_INVALID) > MAX_EVENT_FILES:
            _EVENTS_INVALID.popitem(last=False)
    return out


def diagnostics():
    with _LOCK:
        return {**_STATS, 'run_parts': len(_RUNS), 'index_entries': len(_INDEX),
                'brain_event_files': len(_EVENTS), 'brain_event_meta_files': len(_EVENT_META)}


def clear():
    """Tests and explicit local cache reset only; never edits source files."""
    with _LOCK:
        _RUNS.clear()
        _INDEX.clear()
        _EVENTS.clear()
        _EVENT_META.clear()
        _EVENT_META_SORTED.clear()
        _EVENTS_RO.clear()
        _EVENTS_RO_SORTED.clear()
        _EVENTS_INVALID.clear()
        _SHA.clear()
        for key in _STATS:
            _STATS[key] = 0
