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
# Regenerated for py-1.14.11 (AS1): prompts.py anchor rule gained a line
# clarifying that `i` is the lowercase file-stem slug, never the `#`-display
# id. That line is in the universal core rules → every agent type's briefing
# shifts, on BOTH the empty and populated fixtures.
BRIEFING_GOLDEN_EMPTY = {
    "audit": "6308efd064d0ce2e508436da9808dd5d55858e07b67f21779fd264854d3cbf74",
    "custom": "f5f434dc599cdeb8ca5cf2f11a7bd74bd2e187998818aa696c908dab0dec4790",
    "db": "51291556f9b6e120fddd17b95242b33d154ef28a0dbabcdbb56720e4ccca8d31",
    "deploy": "28fbebfacc86994239912998dd55d41ce5593597034287cd3543fbfb6f3c280f",
    "docs": "2ed71b42e2b1b65bb5eaf538ab41c1c78a907aa2292a858010bf1a59049532f2",
    "review": "86807cf234635b65de6f57e36e8e1ff24ac459e23fc94dcbaaf43552169b9109",
    "roadmap-architect": "661cde5b8cb79b66924a6ff84f2a78fcec6f85fa926763a2900bdb643f9cf554",
    "testing": "f8f28ddcad0f1210ae848f663669cb0d7284157bbae2c4901d5d224ed05860f5",
}

BRIEFING_GOLDEN_POPULATED = {
    "audit": "6e54a7abe20a0e124a8f2c012671d60772f941ef834c453e494a5a53c8bf12df",
    "custom": "3cd80affd62de66f9c9aedfa921efeb497343e1cf1eb9c7811b702c0a9086354",
    "db": "c4307334ac61c4efc29269cdc2447ab9883143b617163a00e5c894e82d5544e3",
    "deploy": "d4bfc0af74bc6cd97d7962c4f784c0f20a231d3a13dc4fecab8ab529e12aeaaf",
    "docs": "f0c2733d849d5bb91457f31a92c0363f23ab9ea19350fda856888367c1ada673",
    "review": "5618a5399b8939b6a41109adc48951013b1357df58c88ec725e1045663f02001",
    "roadmap-architect": "dff2a639017d9a9452eac116a4c4f84d0f285f77cbdacacc04a1a40172466fee",
    "testing": "e3723c56184b8a5532191a1ab41bcb3c37a216aef0ecb2f552339619bab99f6e",
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
    """The module whose namespace `spawn` resolves `_find_claude` against —
    'daemon' before the refactor, 'runner' after DM-modularize, 'runnerspawn'
    after the Phase-3d ChatRunner mixin split. Keyed off spawn's own module so
    the monkeypatch lands wherever spawn is defined."""
    return sys.modules[d.ChatRunner.spawn.__module__]


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
