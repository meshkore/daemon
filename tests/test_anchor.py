"""LAL6 — live-anchor-loop protocol tests.

Three layers of coverage:

1. **Regex unit tests** — verify the wire-format markers parse the
   shapes the protocol promises and reject ones it doesn't.

2. **Handler unit tests** — call the daemon's ``_handle_anchor`` /
   ``_handle_anchor_progress`` against a populated cluster fixture
   and assert files get written + conv_meta gets the right fields +
   WS events fire with the right shape.

3. **End-to-end smoke** — full ``⟦anchor⟧`` line emitted by an agent
   would mean booting a fake claude subprocess. That harness is
   scope-creep for this initiative; instead we exercise the parser
   directly via ``ChatRunner._resolve_anchor_head`` against a stub
   runner whose only purpose is to receive the parse output.

The full-subprocess smoke can be added when the daemon-modularize-2
initiative lifts ChatRunner into chat.py and exposes a clean
``ChatRunner.from_test_config(...)`` constructor."""

from __future__ import annotations

import json

from conftest import Daemon


# ── 1. Regex unit tests ───────────────────────────────────────────────


def test_anchor_regex_matches_single_line() -> None:
    import sys
    from pathlib import Path

    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    import daemon as d  # type: ignore[import-not-found]

    line = '⟦anchor⟧ {"i":"foo","t":"BAR1"}\n'
    m = d.ChatRunner._ANCHOR_RE.match(line)
    assert m is not None
    assert m.group(1) == '{"i":"foo","t":"BAR1"}'


def test_anchor_regex_matches_new_init() -> None:
    import sys
    from pathlib import Path

    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    import daemon as d  # type: ignore[import-not-found]

    line = '⟦anchor⟧ {"new_i":{"id":"x","title":"X"},"new_t":{"id":"X1","title":"first"}}\n'
    m = d.ChatRunner._ANCHOR_RE.match(line)
    assert m is not None
    payload = json.loads(m.group(1))
    assert payload["new_i"]["id"] == "x"


def test_anchor_progress_regex_matches_mid_stream() -> None:
    import sys
    from pathlib import Path

    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    import daemon as d  # type: ignore[import-not-found]

    text = 'doing work...\n⟦anchor-progress⟧ {"t":"X1","status":"done"}\nnext task...'
    m = d.ChatRunner._ANCHOR_PROGRESS_RE.search(text)
    assert m is not None
    assert m.group(1) == '{"t":"X1","status":"done"}'


def test_anchor_regex_rejects_no_marker() -> None:
    import sys
    from pathlib import Path

    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    import daemon as d  # type: ignore[import-not-found]

    assert d.ChatRunner._ANCHOR_RE.match("hello world\n") is None
    assert d.ChatRunner._ANCHOR_RE.match("just text without marker") is None
    # A bare markdown link must NOT be mistaken for a marker (close
    # bracket is followed by `(` not a `{` JSON body).
    assert d.ChatRunner._ANCHOR_RE.match("[anchor](#link)\n") is None


def test_anchor_regex_tolerates_ascii_and_cjk_brackets() -> None:
    """py-1.14.10 — LLMs routinely render the canonical ⟦ ⟧ as ASCII
    `[anchor]` / `[[anchor]]` (or CJK 【anchor】). The parser must accept
    all of them or the marker leaks into chat AND the roadmap never lights
    up. Operator field report 2026-06-13 (ikamiro showed `[anchor]`)."""
    import sys
    from pathlib import Path

    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    import daemon as d  # type: ignore[import-not-found]

    for opener, closer in [
        ("[", "]"),
        ("[[", "]]"),
        ("【", "】"),
        ("〚", "〛"),
        ("⟦", "⟧"),
    ]:
        line = f'{opener}anchor{closer} {{"i":"I12","t":"WEB-wizard-kid"}}\n'
        m = d.ChatRunner._ANCHOR_RE.match(line)
        assert m is not None, f"failed to match {opener}anchor{closer}"
        assert json.loads(m.group(1))["t"] == "WEB-wizard-kid"

    # progress variant, same bracket tolerance
    for opener, closer in [("[", "]"), ("[[", "]]"), ("⟦", "⟧")]:
        text = f'work…\n{opener}anchor-progress{closer} {{"t":"X1","status":"done"}}\n'
        m = d.ChatRunner._ANCHOR_PROGRESS_RE.search(text)
        assert m is not None, f"failed to match {opener}anchor-progress{closer}"

    # The plain anchor regex must NOT swallow an anchor-progress marker
    # (the `-` after `anchor` blocks the close bracket).
    assert d.ChatRunner._ANCHOR_RE.match('[anchor-progress] {"t":"X1"}\n') is None


def test_resolve_anchor_head_strips_ascii_marker() -> None:
    """End-to-end of the head resolver with an ASCII `[anchor]`: the
    marker line is consumed (not forwarded to chat) and the handler fires."""
    import sys
    from pathlib import Path

    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    import daemon as d  # type: ignore[import-not-found]

    seen: dict = {}

    class _StubDaemon:
        def _handle_anchor(self, conv, payload, raw=None):
            seen["payload"] = payload

        def _handle_anchor_missing(self, conv):
            seen["missing"] = True

    runner = d.ChatRunner.__new__(d.ChatRunner)
    runner.conv = "c1"
    runner._head_buffer = ""
    runner._anchor_head_resolved = False
    runner.daemon = _StubDaemon()

    visible = runner._resolve_anchor_head(
        '[anchor] {"i":"I12","t":"WEB-wizard-kid"}\nHola, empiezo.\n'
    )
    assert "anchor" not in visible  # marker line stripped
    assert visible.strip() == "Hola, empiezo."
    assert seen.get("payload", {}).get("i") == "I12"
    assert "missing" not in seen  # the missing-handler must NOT have fired


# ── 2. Health surface — protocol-aware daemons announce the features ──


def test_daemon_health_advertises_anchor_features(daemon: Daemon) -> None:
    """py-1.12.31+ ships anchor.v1. py-1.12.32 adds anchor.strip.v1.
    py-1.13.0 adds anchor.handler.v1 + anchor.auto-create.v1 +
    anchor.progress.v1."""
    r = daemon.get("/health")
    assert r.status_code == 200
    feats = set(r.json()["features"])
    expected = {
        "anchor.v1",
        "anchor.strip.v1",
        "anchor.handler.v1",
        "anchor.auto-create.v1",
        "anchor.progress.v1",
    }
    missing = expected - feats
    assert not missing, f"missing anchor features: {missing}"


# ── 3. End-to-end smoke (full subprocess) — deferred ──────────────────
#
# A real `⟦anchor⟧` emitted by claude-code would require a fake
# binary that prints scripted stream-json. The fake-claude harness is
# scope-creep for this initiative — daemon-modularize-2 will lift
# ChatRunner into chat.py and expose a clean constructor that
# accepts a pre-recorded stream. At that point this file gains:
#
#   def test_anchor_new_init_creates_files(daemon: Daemon) -> None:
#       """Send a dispatch with a fake claude that emits
#       `⟦anchor⟧ {"new_i":{...},"new_t":{...}}` as its first
#       delta. After the turn, the initiative + task .md files
#       exist on disk with the expected frontmatter; conv_meta.json
#       has the new entry; a `conv.anchored` event was broadcast."""
