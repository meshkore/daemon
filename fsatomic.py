"""fsatomic.py — crash-safe atomic file writes (Phase E3 DRY leaf).

Every on-disk JSON/YAML/markdown the daemon persists uses the same dance:
write a sibling ``<name>.tmp``, (optionally) fsync, then ``os.replace`` —
atomic on POSIX, so a crash mid-write leaves the OLD file intact, never a
truncated one that a later read silently parses as empty. That idiom was
copy-pasted across ~9 modules; this leaf is the single source of truth.

Leaf module: stdlib only, no daemon imports. Callers pass the exact JSON
flags they need (``sort_keys`` changes the bytes; ``fsync`` adds durability
without changing content) so the swap is byte-for-byte behaviour-preserving."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any


def atomic_write_text(path: Path, text: str, *, fsync: bool = False) -> None:
    """Write ``text`` to ``path`` atomically via a sibling ``.tmp`` + replace.

    ``fsync=True`` flushes the kernel buffer to disk before the rename (use
    for state that must survive a hard crash — chat queues, archives)."""
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as fh:
        fh.write(text)
        if fsync:
            fh.flush()
            os.fsync(fh.fileno())
    os.replace(tmp, path)


def atomic_write_json(
    path: Path,
    obj: Any,
    *,
    indent: int = 2,
    sort_keys: bool = False,
    fsync: bool = False,
    trailing_newline: bool = False,
) -> None:
    """``json.dumps(obj, …)`` then :func:`atomic_write_text`. Flags mirror the
    call sites so output stays byte-identical to the hand-rolled versions."""
    text = json.dumps(obj, indent=indent, sort_keys=sort_keys)
    if trailing_newline:
        text += "\n"
    atomic_write_text(path, text, fsync=fsync)
