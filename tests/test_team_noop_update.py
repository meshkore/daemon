"""test_team_noop_update.py — saving a member without changing anything must
not touch the file.

Root cause: `TeamStore.team_update` bumped `updated` to today and rewrote
`.meshkore/team/<id>.md` on EVERY PATCH — and the cockpit PATCHes per
section even when the operator changed nothing. So every "save" left a
dirty git diff under `.meshkore/team/`. A no-op PATCH now returns the
member untouched (no write, no stamp bump); only a real change writes.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from team import TeamStore  # noqa: E402


class _Paths:
    def __init__(self, root: Path) -> None:
        self.team_dir = root / "team"


def _store(tmp_path: Path) -> TeamStore:
    return TeamStore(_Paths(tmp_path))


def _seed(store: TeamStore) -> None:
    store.team_create(
        {
            "id": "dev",
            "kind": "profile",
            "client": "claude-code",
            "model": "sonnet",
            "effort": "default",
            "body": "hello",
        },
        today="2026-09-12",
    )


def test_noop_patch_leaves_file_untouched(tmp_path: Path) -> None:
    store = _store(tmp_path)
    _seed(store)
    p = tmp_path / "team" / "dev.md"
    before_bytes = p.read_bytes()
    before_mtime = p.stat().st_mtime_ns

    cur = store.team_get("dev")
    fm = cur["frontmatter"]
    # Same-values patch, as the cockpit sends per section even unchanged.
    out = store.team_update(
        "dev",
        {
            "client": fm["client"],
            "model": fm["model"],
            "effort": fm["effort"],
            "body": cur["body"],
        },
        today="2026-09-13",
    )

    assert p.read_bytes() == before_bytes
    assert p.stat().st_mtime_ns == before_mtime
    assert out["frontmatter"]["updated"] == "2026-09-12"


def test_real_change_writes_and_bumps_updated(tmp_path: Path) -> None:
    store = _store(tmp_path)
    _seed(store)
    p = tmp_path / "team" / "dev.md"
    before_bytes = p.read_bytes()

    out = store.team_update("dev", {"model": "opus"}, today="2026-09-13")

    assert p.read_bytes() != before_bytes
    assert out["frontmatter"]["model"] == "opus"
    assert out["frontmatter"]["updated"] == "2026-09-13"
