"""timeline.py — timeline JSONL read/append helpers.

Extracted from utils.py (daemon-architecture-v2 Phase 3d). Verbatim move;
imported back where used."""

from __future__ import annotations

import json
import os
from typing import Any, Dict, List

from paths import Paths
from timeutil import _iso_now


def _iter_timeline_files(paths: "Paths") -> List[Any]:
    """All timeline files (jsonl + jsonl.gz from rotation)."""
    if not paths.timeline_dir.exists():
        return []
    files = list(paths.timeline_dir.glob("*.jsonl"))
    files.extend(paths.timeline_dir.glob("*.jsonl.gz"))
    # Also look in the archive subdir produced by rotation.
    archive_dir = paths.timeline_dir / "archive"
    if archive_dir.exists():
        files.extend(archive_dir.glob("*.jsonl"))
        files.extend(archive_dir.glob("*.jsonl.gz"))
    return files


def _read_timeline_file(path: Any) -> List[Dict[str, Any]]:
    """Parse one timeline file (jsonl or jsonl.gz) → list of events.
    Never raises; bad lines / unreadable files yield empty list."""
    try:
        if str(path).endswith(".gz"):
            import gzip

            with gzip.open(path, "rt", encoding="utf-8", errors="replace") as fh:
                lines = fh.read().splitlines()
        else:
            lines = path.read_text(errors="replace").splitlines()
    except OSError:
        return []
    out: List[Dict[str, Any]] = []
    for line in lines:
        try:
            out.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return out


def _append_timeline(paths: "Paths", event: Dict[str, Any]) -> Dict[str, Any]:
    """Append one JSON-line event to today's timeline file.
    Returns the event enriched with `ts` if it wasn't already set.

    py-1.5.0 — atomic append. The line is rendered fully in memory,
    then written + flushed + fsync'd in a single open/close cycle so
    a daemon crash mid-write can't leave a half-written line in the
    jsonl. We rely on the OS guarantee that `write()` is atomic for
    buffers < PIPE_BUF (~4KB on most systems); for larger events
    (very long assistant.final replies) we still get atomicity at the
    page-cache level under POSIX. The added fsync forces durability
    so we don't lose events on a power cut either."""
    paths.timeline_dir.mkdir(parents=True, exist_ok=True)
    if "ts" not in event:
        event = {**event, "ts": _iso_now()}
    date = event["ts"][:10]
    f = paths.timeline_dir / f"{date}.jsonl"
    payload = json.dumps(event, separators=(",", ":")) + "\n"
    encoded = payload.encode("utf-8")
    # Open with O_APPEND so concurrent writers (the StateManager poll
    # loop + ChatRunner reader threads) interleave at line boundaries
    # rather than overwrite each other. O_APPEND is atomic per write()
    # on POSIX for any size up to PIPE_BUF; for larger writes (a multi-
    # KB assistant.final) the worst case is interleaved bytes, but the
    # daemon's writers never race on the same line. Single line per
    # write() call preserves jsonl integrity.
    fd = os.open(f, os.O_WRONLY | os.O_APPEND | os.O_CREAT, 0o644)
    try:
        os.write(fd, encoded)
        try:
            os.fsync(fd)
        except OSError:
            pass  # best-effort durability
    finally:
        os.close(fd)
    return event
