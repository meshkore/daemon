"""DAH2 — `/self-update` must replace the binary the daemon IS RUNNING.

The bug this pins (observed live during the py-1.34.0 release, 2026-08-27):
`self_update` resolved its target through `self.paths.scripts_dir`. `self.paths`
is the DC-4 PER-REQUEST accessor — it resolves from the `X-MeshKore-Project`
header. But there is exactly one daemon per machine (Standard v28), so a
`/self-update` is a MACHINE-level operation and has no business following the
caller's project.

Sent with `X-MeshKore-Project: meshkore-main`, the update:

  - downloaded the new bundle into `meshkore/.meshkore/scripts/` — a member
    project that, per the centralized model, must carry NO daemon code at all;
  - "backed up" that project's stale leftover (py-1.30.3, months old) as the
    rollback point, instead of the py-1.33.0 that was actually running, so the
    reported `old_backup` pointed at the wrong bytes;
  - re-exec'd from there, leaving the real install dir a version behind.

Net effect: the daemon's install location wandered between project folders on
every update, and the rollback path was a lie. It "worked" only because any
directory is a valid place to run a self-contained script from.

The fix resolves `sys.argv[0]` — the script this process was launched with,
which is by definition the file that must be swapped.
"""

from __future__ import annotations

import ast
from pathlib import Path

DAEMON_DIR = Path(__file__).resolve().parents[1]


def _self_update_source() -> str:
    return (DAEMON_DIR / "selfupdatesvc.py").read_text()


def test_install_dir_comes_from_argv_not_from_the_request_project() -> None:
    src = _self_update_source()
    assert "Path(sys.argv[0]).resolve().parent" in src, (
        "self_update must resolve the daemon's OWN install dir from sys.argv[0]"
    )
    assert "self.paths.scripts_dir" not in src, (
        "self_update still resolves scripts_dir through the per-request project "
        "accessor — that is the bug: it swaps a file in whichever project the "
        "X-MeshKore-Project header names, not the binary this process is running."
    )


def test_the_swapped_file_is_the_running_script() -> None:
    src = _self_update_source()
    assert "current = Path(sys.argv[0]).resolve()" in src, (
        "the file replaced by the atomic rename must be the running script, "
        "not a `scripts_dir / 'daemon.py'` guess — the running binary is not "
        "always named daemon.py in a directory we picked."
    )


def test_response_reports_the_install_dir() -> None:
    """A wandering install dir was invisible in the response, which is part of
    why it went unnoticed. The 202 now names where the swap happened."""
    src = _self_update_source()
    assert '"install_dir"' in src


def test_module_still_parses() -> None:
    ast.parse(_self_update_source())
