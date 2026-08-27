"""DAH7 — a task closed as `cancelled` must not pin its initiative open.

`_reconcile_initiative_archive` runs on every `/state` build and moves an
initiative between `active` and `done` from the state of its children. It
asked "are all children `done`?" — and `normalize_status` collapses every
value it does not recognise (`cancelled`, `dropped`, `superseded`, and a
dozen others actually used in this repo) to `backlog`, i.e. *pending*.

So one deliberately-closed task held its initiative `active` for good. Found
by hitting it: `daemon-audit-hardening` was set to `done`, and the next state
build reverted it to `active` within seconds because DAH3 had been cancelled
after being measured and found not worth doing. The backward path (py-1.12.4)
is a guard against the architect declaring victory early — correct in intent,
wrong on what counts as "still pending".

Pure-logic tests over the predicate plus the reconciler driven against a
temporary tree; no daemon boot.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from cluster import is_resolved_status  # noqa: E402
from statebuild import _reconcile_initiative_archive  # noqa: E402


# ── the predicate ───────────────────────────────────────────────────────


@pytest.mark.parametrize("status", ["done", "DONE", " Done "])
def test_done_is_resolved(status: str) -> None:
    assert is_resolved_status(status)


@pytest.mark.parametrize(
    "status", ["cancelled", "canceled", "dropped", "wontfix", "superseded", "obsolete"]
)
def test_deliberately_closed_is_resolved(status: str) -> None:
    """These will never become `done`. Treating them as pending is what
    created the bug."""
    assert is_resolved_status(status)


@pytest.mark.parametrize("status", ["backlog", "next", "active", "blocked", "", None])
def test_open_work_is_not_resolved(status: object) -> None:
    assert not is_resolved_status(status)


def test_unknown_status_counts_as_open() -> None:
    """Fail SAFE: an unrecognised status must read as unfinished, so a typo
    can never silently archive live work."""
    assert not is_resolved_status("in-review")


# ── the reconciler ──────────────────────────────────────────────────────


class _Paths:
    def __init__(self, root: Path) -> None:
        self.root = root


def _write_initiative(root: Path, iid: str, status: str) -> dict:
    d = root / ".meshkore" / "roadmap" / "initiatives"
    d.mkdir(parents=True, exist_ok=True)
    rel = f".meshkore/roadmap/initiatives/{iid}.md"
    (root / rel).write_text(
        f"---\nid: {iid}\ntitle: t\nstatus: {status}\n---\n\nbody\n", encoding="utf-8"
    )
    return {"id": iid, "status": status, "path": rel}


def _task(tid: str, iid: str, status: str) -> dict:
    return {"id": tid, "initiative": iid, "status": status}


def _status_on_disk(root: Path, it: dict) -> str:
    for line in (root / it["path"]).read_text(encoding="utf-8").splitlines():
        if line.startswith("status:"):
            return line.split(":", 1)[1].strip()
    raise AssertionError("no status line")


def test_cancelled_child_does_not_reopen_a_done_initiative(tmp_path: Path) -> None:
    """The regression, in the shape it was found: two done, one cancelled."""
    it = _write_initiative(tmp_path, "aud", "done")
    tasks = [
        _task("T1", "aud", "done"),
        _task("T2", "aud", "done"),
        _task("T3", "aud", "cancelled"),
    ]
    _reconcile_initiative_archive([it], tasks, _Paths(tmp_path))
    assert _status_on_disk(tmp_path, it) == "done"


def test_cancelled_child_lets_an_active_initiative_archive(tmp_path: Path) -> None:
    """Forward direction: with the last open task cancelled, there is nothing
    left to do, so the initiative closes on its own."""
    it = _write_initiative(tmp_path, "aud", "active")
    tasks = [_task("T1", "aud", "done"), _task("T2", "aud", "cancelled")]
    _reconcile_initiative_archive([it], tasks, _Paths(tmp_path))
    assert _status_on_disk(tmp_path, it) == "done"


def test_an_all_cancelled_initiative_is_not_completed(tmp_path: Path) -> None:
    """The other direction has to stay honest: abandoning every task is not
    the same as finishing the work, and must not be recorded as `done`."""
    it = _write_initiative(tmp_path, "aud", "active")
    tasks = [_task("T1", "aud", "cancelled"), _task("T2", "aud", "dropped")]
    _reconcile_initiative_archive([it], tasks, _Paths(tmp_path))
    assert _status_on_disk(tmp_path, it) == "active"


def test_a_genuinely_pending_child_still_reverts(tmp_path: Path) -> None:
    """py-1.12.4's guard must survive the fix — the whole point of the
    backward path is that an initiative marked done with real work left is
    corrected."""
    it = _write_initiative(tmp_path, "aud", "done")
    tasks = [_task("T1", "aud", "done"), _task("T2", "aud", "next")]
    _reconcile_initiative_archive([it], tasks, _Paths(tmp_path))
    assert _status_on_disk(tmp_path, it) == "active"


def test_reconcile_is_idempotent(tmp_path: Path) -> None:
    it = _write_initiative(tmp_path, "aud", "active")
    tasks = [_task("T1", "aud", "done"), _task("T2", "aud", "cancelled")]
    for _ in range(3):
        _reconcile_initiative_archive([it], tasks, _Paths(tmp_path))
    assert _status_on_disk(tmp_path, it) == "done"
