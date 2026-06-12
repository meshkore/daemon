"""GET /context + /context/<path> — Standard v14 §3.5 project context
tree. py-1.14.1. The cockpit's Context tab (ContextPanel.tsx) consumes
both; prior to this version the daemon answered 404 on every open."""

from __future__ import annotations

import pytest

from conftest import Daemon


def _find(tree: list, path: str) -> dict | None:
    for n in tree:
        if n.get("path") == path:
            return n
        if n.get("children"):
            hit = _find(n["children"], path)
            if hit:
                return hit
    return None


def test_context_tree_shape(daemon: Daemon) -> None:
    r = daemon.get("/context", headers=daemon.auth)
    assert r.status_code == 200
    d = r.json()
    assert d["exists"] is True
    assert d["root"] == ".meshkore/context"
    assert d["budget_tokens"] == 4500
    assert isinstance(d["tree"], list) and d["tree"]
    assert d["total_words"] > 0
    assert d["token_estimate"] == round(d["total_words"] * 1.5)
    assert d["over_budget"] is False  # tiny fixture is well under budget

    # overview.md is a top-level file with frontmatter title pulled through.
    ov = _find(d["tree"], "overview.md")
    assert ov is not None
    assert ov["kind"] == "file"
    assert ov["title"] == "Overview"
    assert ov["updated"] == "2026-06-06"
    assert ov["status"] == "stable"
    assert ov["words"] > 0
    assert ov["over_cap"] is False

    # decisions/ is a dir with children, README first.
    dec = _find(d["tree"], "decisions")
    assert dec is not None and dec["kind"] == "dir"
    assert dec["children"][0]["name"] == "README.md"
    entry = _find(d["tree"], "decisions/2026-06-06-pick-python.md")
    assert entry is not None
    assert entry["title"] == "Daemon is Python"


def test_context_file_body(daemon: Daemon) -> None:
    r = daemon.get("/context/overview.md", headers=daemon.auth)
    assert r.status_code == 200
    assert "Local-first multi-agent cockpit" in r.text
    assert "text/markdown" in r.headers.get("Content-Type", "")


def test_context_file_nested(daemon: Daemon) -> None:
    r = daemon.get("/context/decisions/2026-06-06-pick-python.md", headers=daemon.auth)
    assert r.status_code == 200
    assert "Decision" in r.text


def test_context_file_traversal_rejected(daemon: Daemon) -> None:
    r = daemon.get("/context/../public/cluster.yaml", headers=daemon.auth)
    assert r.status_code in (400, 404)


@pytest.mark.cluster("empty")
def test_context_absent_is_not_error(daemon: Daemon) -> None:
    """A cluster with no .meshkore/context/ returns exists:false + empty
    tree (NOT 404) so the cockpit renders its bootstrap hint."""
    r = daemon.get("/context", headers=daemon.auth)
    assert r.status_code == 200
    d = r.json()
    assert d["exists"] is False
    assert d["tree"] == []
    assert d["over_budget"] is False
