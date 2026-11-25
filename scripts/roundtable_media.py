#!/usr/bin/env python3
"""Content-addressed media registry for the AI brain.

Only an operator-side CLI can register files. MCP readers can list metadata,
resolve registered IDs, and render still frames from registered videos. Raw
client paths are never accepted by the read API.
"""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import mimetypes
import os
import re
import shutil
import subprocess
from datetime import datetime
from pathlib import Path

SCHEMA = "kingdom.roundtable.media.v1"
MANIFEST_SCHEMA = "kingdom.roundtable.media.manifest.v1"
IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".webp"}
VIDEO_EXTENSIONS = {".mp4", ".mov", ".webm"}
ALLOWED_EXTENSIONS = IMAGE_EXTENSIONS | VIDEO_EXTENSIONS
MAX_BYTES = 256 * 1024 * 1024
SAFE_NAME = re.compile(r"[^0-9A-Za-z._\-ぁ-んァ-ヴー一-龥]+")


def now_iso() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def manifest_path(root: Path) -> Path:
    return root / "roundtable" / "media" / "manifest.jsonl"


def files_dir(root: Path) -> Path:
    return root / "roundtable" / "media" / "files"


def safe_display_name(name: str) -> str:
    cleaned = SAFE_NAME.sub("_", Path(name).name).strip("._")
    return cleaned[:160] or "media"


def _read_jsonl(path: Path) -> list[dict]:
    rows: list[dict] = []
    if not path.is_file():
        return rows
    with path.open(encoding="utf-8", errors="replace") as handle:
        for line in handle:
            try:
                value = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(value, dict):
                rows.append(value)
    return rows


def load_manifest(root: Path) -> list[dict]:
    latest: dict[str, dict] = {}
    for row in _read_jsonl(manifest_path(root)):
        media_id = str(row.get("media_id") or "")
        if media_id:
            latest[media_id] = row
    return sorted(
        latest.values(),
        key=lambda row: (str(row.get("registered_at") or ""), str(row.get("media_id") or "")),
    )


def _ffprobe_binary() -> str:
    candidates = [
        shutil.which("ffprobe"),
        str(Path.home() / "ffmpeg-shared/bin/ffprobe"),
    ]
    for candidate in candidates:
        if candidate and Path(candidate).is_file():
            return str(candidate)
    raise RuntimeError("ffprobe_not_found")


def _ffmpeg_binary() -> str:
    candidates = [
        shutil.which("ffmpeg"),
        str(Path.home() / "ffmpeg-shared/bin/ffmpeg"),
    ]
    for candidate in candidates:
        if candidate and Path(candidate).is_file():
            return str(candidate)
    raise RuntimeError("ffmpeg_not_found")


def _media_env() -> dict[str, str]:
    env = dict(os.environ)
    shared_lib = Path.home() / "ffmpeg-shared/lib"
    if shared_lib.is_dir():
        previous = env.get("LD_LIBRARY_PATH", "")
        env["LD_LIBRARY_PATH"] = str(shared_lib) + ((":" + previous) if previous else "")
    return env


def _image_probe(path: Path) -> dict:
    command = [
        _ffprobe_binary(), "-v", "error",
        "-show_entries", "stream=codec_name,codec_type,width,height",
        "-of", "json", str(path),
    ]
    try:
        payload = json.loads(subprocess.check_output(
            command, text=True, timeout=30, env=_media_env()
        ))
    except Exception as exc:
        raise RuntimeError("invalid_image") from exc
    stream = next((row for row in (payload.get("streams") or [])
                   if row.get("codec_type") == "video"), None)
    if not stream or not stream.get("width") or not stream.get("height"):
        raise RuntimeError("invalid_image")
    return {
        "width": int(stream["width"]),
        "height": int(stream["height"]),
        "image_format": str(stream.get("codec_name") or ""),
    }


def _video_probe(path: Path) -> dict:
    command = [
        _ffprobe_binary(), "-v", "error",
        "-show_entries",
        "format=duration,size,format_name:stream=index,codec_type,codec_name,width,height,r_frame_rate,nb_frames",
        "-of", "json", str(path),
    ]
    try:
        payload = json.loads(subprocess.check_output(
            command, text=True, timeout=30, env=_media_env()
        ))
    except Exception as exc:
        raise RuntimeError("invalid_video") from exc
    streams = payload.get("streams") or []
    video = next((row for row in streams if row.get("codec_type") == "video"), None)
    if not video or not video.get("width") or not video.get("height"):
        raise RuntimeError("video_stream_missing")
    audio = next((row for row in streams if row.get("codec_type") == "audio"), None)
    fmt = payload.get("format") or {}
    return {
        "width": int(video.get("width") or 0),
        "height": int(video.get("height") or 0),
        "duration_seconds": round(float(fmt.get("duration") or 0.0), 6),
        "frame_rate": str(video.get("r_frame_rate") or ""),
        "frame_count": int(video.get("nb_frames") or 0) if str(video.get("nb_frames") or "").isdigit() else None,
        "video_codec": str(video.get("codec_name") or ""),
        "audio_codec": str(audio.get("codec_name") or "") if audio else "",
        "format_name": str(fmt.get("format_name") or ""),
    }


def probe_media(path: Path) -> tuple[str, str, dict]:
    suffix = path.suffix.lower()
    if suffix in IMAGE_EXTENSIONS:
        kind = "image"
        metadata = _image_probe(path)
    elif suffix in VIDEO_EXTENSIONS:
        kind = "video"
        metadata = _video_probe(path)
    else:
        raise RuntimeError("unsupported_media_extension")
    mime_type = mimetypes.guess_type(path.name)[0] or (
        "image/jpeg" if suffix in {".jpg", ".jpeg"} else "application/octet-stream"
    )
    return kind, mime_type, metadata


def _relative_or_absolute(root: Path, source: Path) -> str:
    try:
        return str(source.resolve().relative_to(root.resolve()))
    except ValueError:
        return str(source.resolve())


def _public_row(row: dict) -> dict:
    keys = (
        "schema", "media_id", "display_name", "kind", "mime_type", "sha256", "bytes",
        "width", "height", "duration_seconds", "frame_rate", "frame_count", "video_codec",
        "audio_codec", "event_id", "thread_id", "sender", "role", "tags", "source_path",
        "storage_relpath", "registered_at",
    )
    return {key: row[key] for key in keys if row.get(key) not in (None, "", [])}


def register_media(
    root: Path,
    source: Path,
    *,
    display_name: str = "",
    event_id: str = "",
    thread_id: str = "",
    sender: str = "",
    role: str = "reference",
    tags: list[str] | None = None,
) -> dict:
    root = root.resolve()
    source = source.expanduser()
    if source.is_symlink() or not source.is_file():
        raise RuntimeError("source_must_be_regular_file")
    source = source.resolve()
    size = source.stat().st_size
    if size <= 0 or size > MAX_BYTES:
        raise RuntimeError("media_size_out_of_range")
    kind, mime_type, metadata = probe_media(source)
    digest = sha256_file(source)
    name = safe_display_name(display_name or source.name)
    suffix = source.suffix.lower()
    identity = "|".join((digest, event_id, thread_id, role, name))
    media_id = f"media-{digest[:20]}-{sha256_text(identity)[:8]}"
    destination_dir = files_dir(root)
    destination_dir.mkdir(parents=True, exist_ok=True)
    destination = destination_dir / f"{digest[:20]}-{name}"
    if destination.suffix.lower() != suffix:
        destination = destination.with_suffix(suffix)
    if destination.exists():
        if sha256_file(destination) != digest:
            raise RuntimeError("content_address_collision")
    else:
        temporary = destination.with_name(destination.name + f".tmp-{os.getpid()}")
        shutil.copyfile(source, temporary)
        os.chmod(temporary, 0o640)
        if sha256_file(temporary) != digest:
            temporary.unlink(missing_ok=True)
            raise RuntimeError("copy_hash_mismatch")
        os.replace(temporary, destination)

    row = {
        "schema": SCHEMA,
        "media_id": media_id,
        "display_name": name,
        "kind": kind,
        "mime_type": mime_type,
        "sha256": digest,
        "bytes": size,
        "event_id": event_id,
        "thread_id": thread_id,
        "sender": sender,
        "role": role,
        "tags": sorted({str(tag).strip() for tag in (tags or []) if str(tag).strip()}),
        "source_path": _relative_or_absolute(root, source),
        "storage_relpath": str(destination.relative_to(root)),
        "registered_at": now_iso(),
        **metadata,
    }
    path = manifest_path(root)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+", encoding="utf-8") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        handle.seek(0)
        existing = [json.loads(line) for line in handle if line.strip()]
        same = next((item for item in existing if item.get("media_id") == media_id), None)
        if same:
            return _public_row(same)
        handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")
        handle.flush()
        os.fsync(handle.fileno())
        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
    return _public_row(row)


def list_media(root: Path, query: str = "", kind: str = "", limit: int = 20) -> list[dict]:
    query_norm = query.casefold().strip()
    kind_norm = kind.casefold().strip()
    results = []
    for row in reversed(load_manifest(root)):
        if kind_norm and str(row.get("kind") or "").casefold() != kind_norm:
            continue
        haystack = " ".join(str(row.get(key) or "") for key in (
            "media_id", "display_name", "kind", "mime_type", "event_id", "thread_id",
            "sender", "role", "tags", "source_path",
        )).casefold()
        if query_norm and query_norm not in haystack:
            continue
        results.append(_public_row(row))
        if len(results) >= max(1, min(int(limit), 100)):
            break
    return results


def resolve_media(root: Path, media_id_or_name: str) -> tuple[dict, Path]:
    key = Path(str(media_id_or_name or "")).name
    matches = [row for row in load_manifest(root) if row.get("media_id") == key]
    if not matches:
        matches = [row for row in load_manifest(root) if row.get("display_name") == key]
    if not matches:
        raise RuntimeError("media_not_registered")
    row = matches[-1]
    relative = Path(str(row.get("storage_relpath") or ""))
    if relative.is_absolute() or ".." in relative.parts:
        raise RuntimeError("invalid_storage_path")
    target = (root / relative).resolve()
    try:
        target.relative_to(files_dir(root).resolve())
    except ValueError as exc:
        raise RuntimeError("storage_path_outside_media_store") from exc
    if not target.is_file() or target.is_symlink():
        raise RuntimeError("registered_media_missing")
    if sha256_file(target) != str(row.get("sha256") or ""):
        raise RuntimeError("registered_media_hash_mismatch")
    return _public_row(row), target


def media_frame_bytes(root: Path, media_id_or_name: str, seconds: float = 0.0) -> tuple[bytes, str, dict]:
    row, target = resolve_media(root, media_id_or_name)
    if row.get("kind") == "image":
        image_format = "jpeg" if target.suffix.lower() in {".jpg", ".jpeg"} else target.suffix.lower().lstrip(".")
        return target.read_bytes(), image_format, row
    duration = float(row.get("duration_seconds") or 0.0)
    position = max(0.0, float(seconds))
    if duration > 0:
        position = min(position, max(0.0, duration - 0.001))
    command = [
        _ffmpeg_binary(), "-hide_banner", "-loglevel", "error", "-ss", f"{position:.6f}",
        "-i", str(target), "-frames:v", "1", "-f", "image2pipe", "-vcodec", "png", "-",
    ]
    try:
        payload = subprocess.check_output(command, timeout=45, env=_media_env())
    except Exception as exc:
        raise RuntimeError("video_frame_extract_failed") from exc
    if not payload.startswith(b"\x89PNG\r\n\x1a\n"):
        raise RuntimeError("video_frame_is_not_png")
    return payload, "png", row


def attachments_by_event(root: Path) -> dict[str, list[dict]]:
    output: dict[str, list[dict]] = {}
    for row in load_manifest(root):
        event_id = str(row.get("event_id") or "")
        if event_id:
            output.setdefault(event_id, []).append(_public_row(row))
    return output


def audit(root: Path) -> dict:
    errors: list[str] = []
    rows = load_manifest(root)
    seen: set[str] = set()
    for row in rows:
        media_id = str(row.get("media_id") or "")
        if not media_id or media_id in seen:
            errors.append(f"duplicate_or_missing_media_id:{media_id}")
        seen.add(media_id)
        try:
            resolved, target = resolve_media(root, media_id)
            if int(resolved.get("bytes") or 0) != target.stat().st_size:
                errors.append(f"size_mismatch:{media_id}")
        except RuntimeError as exc:
            errors.append(f"{media_id}:{exc}")
    return {
        "schema": MANIFEST_SCHEMA + ".audit",
        "status": "ok" if not errors else "error",
        "count": len(rows),
        "errors": errors,
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="AI頭脳の画像・動画台帳")
    parser.add_argument("--root", type=Path, default=Path.home() / "local-ai-stack")
    commands = parser.add_subparsers(dest="command", required=True)
    register = commands.add_parser("register")
    register.add_argument("--source", type=Path, required=True)
    register.add_argument("--name", default="")
    register.add_argument("--event-id", default="")
    register.add_argument("--thread-id", default="")
    register.add_argument("--sender", default="")
    register.add_argument("--role", default="reference")
    register.add_argument("--tag", action="append", default=[])
    listing = commands.add_parser("list")
    listing.add_argument("--query", default="")
    listing.add_argument("--kind", default="")
    listing.add_argument("--limit", type=int, default=20)
    info = commands.add_parser("info")
    info.add_argument("key")
    commands.add_parser("audit")
    return parser


def main() -> None:
    args = _parser().parse_args()
    if args.command == "register":
        result = register_media(
            args.root, args.source, display_name=args.name, event_id=args.event_id,
            thread_id=args.thread_id, sender=args.sender, role=args.role, tags=args.tag,
        )
    elif args.command == "list":
        result = list_media(args.root, args.query, args.kind, args.limit)
    elif args.command == "info":
        result = resolve_media(args.root, args.key)[0]
    else:
        result = audit(args.root)
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
