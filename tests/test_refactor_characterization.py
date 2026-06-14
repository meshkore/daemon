"""Golden-master characterization for the daemon-modularize-2 refactor.

This refactor is PURE code movement: ChatRunner → runner.py and the
prompt machinery (AGENT_PROMPTS / manifests / ProjectState /
StateIntegrityChecker / BriefingPipeline) → prompts.py, with a handful
of shared pure helpers relocated to utils.py. Nothing about runtime
behaviour may change.

These tests pin the observable surface of the moved code BEFORE the
move, so re-running them AFTER proves byte-identical behaviour:

* ``BriefingPipeline.build()`` — a SHA-256 of the full composed prompt
  for every agent type, against the ``empty`` and ``populated`` cluster
  fixtures. Any drift in a single prompt byte trips the matching hash.
* ``_agent_manifest`` over every agent type (model / platform /
  quota_key contract that QuotaProber depends on).
* the two ``_agent_type_*`` resolvers.
* ``_session_id_for_conv`` determinism (claude `--session-id` stability).
* anchor-marker stripping (``_strip_all_anchor_markers``) — the wire
  invariant that anchor JSON never leaks into the chat bubble.
* ``ChatRunner.spawn`` argv — the MP1/MP3 ``--model`` / ``--effort``
  pass-through, captured by faking ``subprocess.Popen``.

All symbols are reached via ``daemon as d`` on purpose: daemon.py is
the historical public module and must keep re-exporting them after the
extraction. The golden values were captured against daemon.py at
py-1.14.3 (pre-refactor).
"""

from __future__ import annotations

import hashlib
import io
import json
import subprocess
import sys
from pathlib import Path
from typing import Any, Callable

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import daemon as d  # type: ignore[import-not-found]  # noqa: E402


# ── Golden values (captured from daemon.py @ py-1.14.3) ────────────────

# Hashes are computed over the briefing with the (run-specific) absolute
# cluster root path replaced by "<ROOT>" — see _briefing_hash.
BRIEFING_GOLDEN_EMPTY = {
    "audit": "0467662ac37ce4a4790209ff45052eaa04298d97e2e2f1f2cf294373c4d7918b",
    "custom": "6d65527403f0f88b7e69e08be17d05ff1f84bfeb2383ed073d12d6716c43fc2a",
    "db": "ca173a91968673d0a6a38cc7aa5b8720abcba1328ad1ae583df933d3c7883329",
    "deploy": "29e552e8760158fb1ff233f7764a24ecdbb4b4ab9f6bcdbc5e8a5abf159badaa",
    "docs": "7da216038a7a3e17eece1cd26002a5dcd146147e49813a577bb0e5097e912366",
    "review": "75f11d855ead1ccebbab09a3633afe0ae246a3858ed2dd3d1ddae8ebd4e8604c",
    "roadmap-architect": "c6ec58e7ef59cc857681d0afb48f749cd51b8b59f43c38e4cad9a6660c5ba809",
    "testing": "2db40888cb1f84413743f28b7b539214fdf6b12dc8886ace357641c6e078354c",
}

# Regenerated for Standard §3.5 / v25: BriefingPipeline now prepends the
# `=== PROJECT CONTEXT === … === END CONTEXT ===` block (prompts.py
# _section_project_context). The populated fixture has a .meshkore/context/
# tree, so every agent type's briefing now carries it. The EMPTY golden is
# unchanged (no context/ → empty section).
BRIEFING_GOLDEN_POPULATED = {
    "audit": "a358f145911ea4c9ed11a7913cf15f41fe20e079bb4d35ffd522dfa395aa9a92",
    "custom": "0b9820597e1553d97c541d62e6ef11d1a743b1f7323ca29f8bc02d93eb688e26",
    "db": "e95fefe2395c217999e7ee8d93eb40729d00401825d47a67332ac1da796d7c31",
    "deploy": "6577fe2fd5b28a8b755b4fa7407d451035730752dc00c37e4d71ef5b7fd79163",
    "docs": "e9198c3b4cdaa397a97a6be6e580a842e37b950f612c760160d16b86c5beb753",
    "review": "64df69f53b59f95ae37ed2d27650bd85bd9b69d3a1c3b2321a1cfdf42cd2b23f",
    "roadmap-architect": "aeb9b08616013dd5443f2116f11624ebf5f9797163658b5bf1a8eaf705f53454",
    "testing": "4ea007a638d6cca5c3fc0979003f333dd8171ed625ca30fab96f4eec8879a997",
}

AGENT_KEYS_GOLDEN = [
    "audit",
    "custom",
    "db",
    "deploy",
    "docs",
    "review",
    "roadmap-architect",
    "testing",
]

MANIFEST_HASH_GOLDEN = (
    "cfc649bd959544874d44a289aa774aab09e615911c7a6babcf0142c3a807a745"
)

SESSION_ID_ABC_GOLDEN = "1c0ba2b4-0f92-5065-bd75-b116f72cdec6"


def _briefing_hash(paths: Any, cluster: Any, agent_type: str) -> str:
    bp = d.BriefingPipeline(
        paths=paths,
        cluster=cluster,
        identity="id-x",
        conv="conv-x",
        user_text="hello world",
        agent_type=agent_type,
    )
    # Normalise out the two run/version-specific spans so the hash is a
    # stable refactor guard: the absolute cluster root (Role section) and
    # the live DAEMON_VERSION (architect commit-trailer SOP). The latter
    # keeps the golden valid across version bumps.
    brief = (
        bp.build().replace(str(paths.root), "<ROOT>").replace(d.DAEMON_VERSION, "<VER>")
    )
    return hashlib.sha256(brief.encode()).hexdigest()


# ── Prompt registry contract ──────────────────────────────────────────


def test_agent_prompts_keys_frozen() -> None:
    assert sorted(d.AGENT_PROMPTS.keys()) == AGENT_KEYS_GOLDEN


def test_agent_manifest_snapshot() -> None:
    """QuotaProber keys agents by `_agent_manifest(t)['quota_key']`. Pin
    the full manifest of every type so a prompt move can't perturb it."""
    man = {t: d._agent_manifest(t) for t in sorted(d.AGENT_PROMPTS)}
    h = hashlib.sha256(json.dumps(man, sort_keys=True).encode()).hexdigest()
    assert h == MANIFEST_HASH_GOLDEN
    # Sanity on the contract shape itself (so a failure is debuggable).
    assert man["custom"] == {
        "model": "auto",
        "platform": "claude-code",
        "quota_key": "claude-code/auto",
    }


def test_agent_type_normalised() -> None:
    assert d._agent_type_normalised("custom") == "custom"
    assert d._agent_type_normalised("roadmap-architect") == "roadmap-architect"
    assert d._agent_type_normalised(None) == "custom"
    assert d._agent_type_normalised("does-not-exist") == "custom"


def test_agent_type_from_conv_slug() -> None:
    assert d._agent_type_from_conv_slug("roadmap-architect-abc") == "roadmap-architect"
    assert d._agent_type_from_conv_slug("general-06051916") is None


# ── BriefingPipeline golden master ─────────────────────────────────────


def test_briefing_build_golden_empty(cluster: Callable[[str], Path]) -> None:
    root = cluster("empty")
    paths = d.Paths(root)
    clu = d.Cluster(paths)
    for at in sorted(d.AGENT_PROMPTS):
        assert _briefing_hash(paths, clu, at) == BRIEFING_GOLDEN_EMPTY[at], (
            f"briefing drift for agent_type={at} on empty cluster"
        )


def test_briefing_build_golden_populated(cluster: Callable[[str], Path]) -> None:
    root = cluster("populated")
    paths = d.Paths(root)
    clu = d.Cluster(paths)
    for at in sorted(d.AGENT_PROMPTS):
        assert _briefing_hash(paths, clu, at) == BRIEFING_GOLDEN_POPULATED[at], (
            f"briefing drift for agent_type={at} on populated cluster"
        )


# ── Shared helpers that move to utils.py ───────────────────────────────


def test_session_id_deterministic() -> None:
    assert d._session_id_for_conv("abc") == SESSION_ID_ABC_GOLDEN
    # Same conv → same id (claude prompt-cache stability across restarts).
    assert d._session_id_for_conv("abc") == d._session_id_for_conv("abc")
    assert d._session_id_for_conv("abc") != d._session_id_for_conv("xyz")


def test_parse_simple_yaml_still_on_daemon() -> None:
    """parse_simple_yaml moves to utils but daemon must re-export it —
    Cluster() + every frontmatter read goes through `daemon.parse_*`."""
    assert d.parse_simple_yaml("a: 1\nb: [x, y]\n") == {"a": 1, "b": ["x", "y"]}
    assert d.parse_frontmatter("---\nid: foo\n---\nbody\n") == {"id": "foo"}


# ── Anchor wire invariant ──────────────────────────────────────────────


def test_strip_all_anchor_markers() -> None:
    """py-1.13.2 belt-and-suspenders strip: neither marker kind may
    survive into the persisted/broadcast final text."""
    runner = _bare_runner()
    text = (
        '⟦anchor⟧ {"i":"alpha","t":"T1"}\n'
        "Here is the real answer.\n"
        '⟦anchor-progress⟧ {"t":"T1","status":"done"}\n'
        "Done."
    )
    out = runner._strip_all_anchor_markers(text)
    assert "⟦anchor⟧" not in out
    assert "⟦anchor-progress⟧" not in out
    assert "Here is the real answer." in out
    assert "Done." in out


# ── ChatRunner.spawn argv — MP1/MP3 model + effort pass-through ─────────


class _FakeHub:
    def broadcast(self, *a: Any, **k: Any) -> None:
        pass


class _FakeProc:
    """Records the argv, then behaves like a process that exited 0 with
    empty stdout/stderr so the reader threads finalize immediately."""

    last_args: list[str] = []

    def __init__(self, args: list[str], **kw: Any) -> None:
        type(self).last_args = list(args)
        self.pid = 4242
        self.stdin = io.BytesIO()
        self.stdout = io.BytesIO(b"")
        self.stderr = io.BytesIO(b"")

    def poll(self) -> int:
        return 0

    def wait(self, timeout: float | None = None) -> int:
        return 0

    def kill(self) -> None:
        pass


def _runner_module() -> Any:
    """The module that currently OWNS ChatRunner — 'daemon' before the
    refactor, 'runner' after. Patching its `_find_claude` global works
    in both worlds (the bare call resolves against the owning module)."""
    return sys.modules[d.ChatRunner.__module__]


def _build_runner(cluster_root: Path, **kw: Any) -> Any:
    paths = d.Paths(cluster_root)
    clu = d.Cluster(paths)
    return d.ChatRunner(
        paths=paths,
        cluster=clu,
        hub=_FakeHub(),
        identity="id-x",
        conv="conv-x",
        prompt="hi",
        daemon=None,
        **kw,
    )


def _bare_runner() -> Any:
    """A ChatRunner with no real cluster — enough to call the pure
    string-stripping methods (they touch no filesystem)."""
    return d.ChatRunner(
        paths=d.Paths(Path("/tmp")),
        cluster=object.__new__(d.Cluster),  # never .reload()'d; unused here
        hub=_FakeHub(),
        identity="i",
        conv="c",
        prompt="p",
        daemon=None,
    )


def _spawn_capture(cluster_root: Path, monkeypatch: Any, **runner_kw: Any) -> list[str]:
    monkeypatch.setattr(subprocess, "Popen", _FakeProc)
    monkeypatch.setattr(_runner_module(), "_find_claude", lambda: "/usr/bin/claude")
    runner = _build_runner(cluster_root, **runner_kw)
    runner.spawn()
    runner.done.wait(timeout=5)
    return _FakeProc.last_args


def test_spawn_argv_passes_model_and_effort(
    cluster: Callable[[str], Path], monkeypatch: Any
) -> None:
    root = cluster("populated")
    args = _spawn_capture(root, monkeypatch, model="opus", effort="high")
    assert "--model" in args
    assert args[args.index("--model") + 1] == "opus"
    assert "--effort" in args
    assert args[args.index("--effort") + 1] == "high"


def test_spawn_argv_omits_model_and_effort_when_unset(
    cluster: Callable[[str], Path], monkeypatch: Any
) -> None:
    root = cluster("populated")
    args = _spawn_capture(root, monkeypatch, model=None, effort=None)
    assert "--model" not in args
    assert "--effort" not in args
    # The stable scaffold is always present regardless of model/effort.
    assert "--output-format" in args and "stream-json" in args
