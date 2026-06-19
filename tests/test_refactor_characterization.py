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
# Regenerated for py-1.24.2 (AS2 anchor reuse-archived-stories): prompts.py's
# anchor decision chain now tells the agent to also read initiatives/log/ and
# prefer reusing an archived story over a near-duplicate. That block is in the
# universal core rules → every agent type's briefing shifts, on BOTH the empty
# and populated fixtures.
BRIEFING_GOLDEN_EMPTY = {
    "audit": "a70caec4ffd2a526452564ae1e2400462a60d605a4fa774581e9df7805db88f7",
    "custom": "3335292bfc5bb92c79865c17d509664b385009a570ee946b7981f78efb36f4ad",
    "db": "7ad5077e12180c0e8cb7550a68236ea1eb6f3d7548f0d6fc2d9bf922ad7d0924",
    "deploy": "05d088d32474513986d6ed77062630fcd55d777f497da61a91f7620294239708",
    "docs": "0dbcc72f30a06bfd8a37ea7f75aad74a00b92fc27251a6d45bd4613439878cd9",
    "review": "848e505c7365d22a583e085520a4d8b5259622fed5a0c440fff303640f7b04ca",
    "roadmap-architect": "e1ea3918b2caa44d36e7294431dfb2c752d1e5b60b793c364370f3b06c389a24",
    "testing": "fa84cf793f6f83a63f714b03330bd02c54c62790787b4b4f7286a660a37ecf73",
}

BRIEFING_GOLDEN_POPULATED = {
    "audit": "40f85d10e03aa62ac1ba61bc14d7f2b006a4cbf6dbcbf8069c6f9a3f20cc8d9e",
    "custom": "5309a9b3ba1a909427f9623eb59252c6ef6b564615b4b3bb2c4e48c9f1490132",
    "db": "3423070046b4ee1a2ac40dcc65a19d751a938675f658471895dd92a76d893926",
    "deploy": "53c913aaf398df858bb48be42d9584f399f7c6f9174995fc8977ecbc92a1b6fe",
    "docs": "dda6f1abf5f7b53256b0b21186e8208d3224c2c6c136f3770cd8387d285a4480",
    "review": "328f96510fb5754f354321d69d32555a94ee0bbd90d176ca0309ac68a9d2f01a",
    "roadmap-architect": "8490bb757b05af0e5c7801c2cc8051a0f83b2031f93c7e81e3557827a95ed51c",
    "testing": "3db7ba2a711f3b172956a9814a5e7e7c1597979d95758d90c41ceded10548fa0",
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
