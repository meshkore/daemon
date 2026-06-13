"""§17 agent-instructions renderer (render.py / AgentInstructionsRenderer).

Pure in-process tests — no daemon subprocess. Exercises the two jobs:
per-CLI render (idempotent, v19-gated) and the preamble splice (must
preserve OPERATOR_CONTENT byte-for-byte)."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from paths import Paths  # noqa: E402
from render import _TARGETS, AgentInstructionsRenderer  # noqa: E402


class _DummyHub:
    def __init__(self) -> None:
        self.events = []

    def broadcast(self, ev: dict) -> None:
        self.events.append(ev)


_PREAMBLE = "# Canonical preamble\n\nRule 1.\nRule 2.\n"
_OPERATOR = (
    "<!-- OPERATOR_CONTENT_BEGIN — this is your project. Edit freely. -->\n\n"
    "# my-project\n\nProject rule A. Project rule B.\n\n"
    "<!-- OPERATOR_CONTENT_END -->\n"
)


def _source_text(preamble: str = _PREAMBLE) -> str:
    return (
        "<!-- MESHKORE_PREAMBLE_BEGIN — managed by the daemon, do not hand-edit -->\n\n"
        f"{preamble.strip()}\n\n"
        "<!-- MESHKORE_PREAMBLE_END -->\n\n"
        f"{_OPERATOR}"
    )


def _build_cluster(root: Path, std_version: int = 24) -> Paths:
    (root / ".meshkore" / "public").mkdir(parents=True, exist_ok=True)
    (root / ".meshkore" / "public" / "AGENT_INSTRUCTIONS.md").write_text(_source_text())
    (root / ".meshkore" / "STANDARD_VERSION").write_text(f"{std_version}\n")
    return Paths(root)


def test_render_targets_v24_writes_all_five(tmp_path: Path) -> None:
    paths = _build_cluster(tmp_path, std_version=24)
    r = AgentInstructionsRenderer(paths, _DummyHub())  # boot render runs in __init__
    r.shutdown()
    for rel, audience, _min in _TARGETS:
        dest = tmp_path / rel
        assert dest.exists(), f"{rel} not rendered"
        txt = dest.read_text()
        assert txt.startswith("<!-- Auto-rendered from")
        assert f"Audience: {audience}" in txt
        # parity: everything after the 4-line header equals the source
        body = "\n".join(txt.splitlines()[4:]) + "\n"
        assert body == paths.public.joinpath("AGENT_INSTRUCTIONS.md").read_text()


def test_render_is_idempotent(tmp_path: Path) -> None:
    paths = _build_cluster(tmp_path, std_version=24)
    r = AgentInstructionsRenderer(paths, _DummyHub())
    again = r.render_targets()  # nothing changed since boot render
    r.shutdown()
    assert again == [], f"expected no rewrites, got {again}"


def test_v19_targets_gated_below_19(tmp_path: Path) -> None:
    paths = _build_cluster(tmp_path, std_version=18)
    r = AgentInstructionsRenderer(paths, _DummyHub())
    r.shutdown()
    assert (tmp_path / "CLAUDE.md").exists()
    assert not (tmp_path / ".clinerules").exists()
    assert not (tmp_path / ".cursor" / "rules" / "meshkore.mdc").exists()


def test_refresh_replaces_preamble_preserves_operator(tmp_path: Path) -> None:
    paths = _build_cluster(tmp_path, std_version=24)
    r = AgentInstructionsRenderer(paths, _DummyHub())
    new_preamble = "# Canonical preamble v2\n\nRule 1.\nRule 2.\nRule 3 (new).\n"
    r._fetch_canonical = lambda: new_preamble  # type: ignore[assignment]
    changed = r.refresh_from_remote()
    r.shutdown()
    assert changed is True
    src = (paths.public / "AGENT_INSTRUCTIONS.md").read_text()
    assert "Rule 3 (new)." in src  # preamble updated
    assert "Project rule A. Project rule B." in src  # operator block preserved
    assert src.count("OPERATOR_CONTENT_BEGIN") == 1
    assert src.count("MESHKORE_PREAMBLE_END") == 1
    # second refresh with identical content is a no-op
    assert r.refresh_from_remote() is False
    # the per-CLI file picked up the new rule
    assert "Rule 3 (new)." in (tmp_path / "CLAUDE.md").read_text()


def test_standard_drift_detection(tmp_path: Path) -> None:
    paths = _build_cluster(tmp_path, std_version=24)
    hub = _DummyHub()
    r = AgentInstructionsRenderer(paths, hub)
    # local 24, latest 25 → drift
    r._fetch_standard_version = lambda: 25  # type: ignore[assignment]
    assert r.check_standard_drift() is True
    assert r.standard_drift is True
    assert r.local_standard_version == 24
    assert r.latest_standard_version == 25
    assert any(e.get("type") == "standard.drift" for e in hub.events)
    # second poll while still drifted does NOT re-broadcast
    n = len([e for e in hub.events if e.get("type") == "standard.drift"])
    r.check_standard_drift()
    assert len([e for e in hub.events if e.get("type") == "standard.drift"]) == n
    r.shutdown()


def test_no_drift_when_current(tmp_path: Path) -> None:
    paths = _build_cluster(tmp_path, std_version=25)
    r = AgentInstructionsRenderer(paths, _DummyHub())
    r._fetch_standard_version = lambda: 25  # type: ignore[assignment]
    assert r.check_standard_drift() is False
    assert r.standard_drift is False
    # a failed fetch (None) must not flip drift or crash
    r._fetch_standard_version = lambda: None  # type: ignore[assignment]
    assert r.check_standard_drift() is False
    r.shutdown()
