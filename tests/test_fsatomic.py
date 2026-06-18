"""atomic write helper (Phase E3). Locks the contract the 7+ call sites rely on:
the final file is the full content, the .tmp never lingers, a write over an
existing file replaces it, and byte output matches the hand-rolled json.dumps."""

from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from fsatomic import atomic_write_json, atomic_write_text  # noqa: E402


def test_text_roundtrip_and_no_tmp(tmp_path: Path) -> None:
    p = tmp_path / "x.md"
    atomic_write_text(p, "hello\n")
    assert p.read_text() == "hello\n"
    assert not (tmp_path / "x.md.tmp").exists()


def test_text_replaces_existing(tmp_path: Path) -> None:
    p = tmp_path / "x.yaml"
    p.write_text("old")
    atomic_write_text(p, "new")
    assert p.read_text() == "new"


def test_json_bytes_match_handrolled(tmp_path: Path) -> None:
    obj = {"b": 2, "a": [1, 2], "updated_at": "z"}
    p = tmp_path / "s.json"
    atomic_write_json(p, obj, sort_keys=True)
    assert p.read_text() == json.dumps(obj, indent=2, sort_keys=True)
    # default (no sort) matches the no-sort_keys call sites
    p2 = tmp_path / "s2.json"
    atomic_write_json(p2, obj)
    assert p2.read_text() == json.dumps(obj, indent=2)


def test_json_trailing_newline(tmp_path: Path) -> None:
    p = tmp_path / "ports.json"
    atomic_write_json(p, {"x": 1}, sort_keys=True, trailing_newline=True)
    assert p.read_text() == json.dumps({"x": 1}, indent=2, sort_keys=True) + "\n"


def test_fsync_path_writes(tmp_path: Path) -> None:
    p = tmp_path / "q.json"
    atomic_write_json(p, {"items": []}, fsync=True)
    assert json.loads(p.read_text()) == {"items": []}
