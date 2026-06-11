"""Pure-text helpers from daemon.py — no daemon boot required.

These functions become daemon/prompts.py in DM7; pinning them as pure
functions now means the extraction is a copy-paste, not a redesign."""

from __future__ import annotations

import sys
from pathlib import Path

# Import daemon.py as a module so we can call its helpers in-process.
# This adds ~1k LOC to the test interpreter, but tests are short-lived
# and the alternative (subprocess invocation per helper) would be
# absurdly slow.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import daemon as d  # type: ignore[import-not-found]  # noqa: E402


def test_daemon_version_format() -> None:
    """py-<X.Y.Z>. Anything else breaks the cockpit's version-gap detector."""
    assert d.DAEMON_VERSION.startswith("py-")
    parts = d.DAEMON_VERSION[3:].split(".")
    assert len(parts) >= 2 and all(p.isdigit() for p in parts[:3])


def test_parse_simple_yaml_basic() -> None:
    """The daemon's tiny YAML subset is load-bearing — cluster.yaml +
    every initiative's frontmatter parses through it. Pin the contract."""
    out = d.parse_simple_yaml("version: 1\nid: foo\ntype: dev\n")
    assert out == {"version": 1, "id": "foo", "type": "dev"}


def test_parse_simple_yaml_list() -> None:
    out = d.parse_simple_yaml("modules: [daemon, webapp, architect]\n")
    assert out == {"modules": ["daemon", "webapp", "architect"]}


def test_agent_type_normalised() -> None:
    """conv slugs imply their agent_type. roadmap-architect-* → roadmap-architect."""
    inferred = d._agent_type_from_conv_slug("roadmap-architect-abc")
    assert inferred == "roadmap-architect"
    assert d._agent_type_from_conv_slug("general-06051916") is None


def test_iso_now_format() -> None:
    """ISO-8601 UTC with Z suffix. The cockpit sorts on this string
    lexicographically — any deviation breaks the chat rail order."""
    ts = d._iso_now()
    assert ts.endswith("Z")
    assert "T" in ts
    # Lexicographically sortable: 2026 > 2025.
    assert ts >= "2026-01-01T00:00:00Z"
