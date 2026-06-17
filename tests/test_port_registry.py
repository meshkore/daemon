"""Stable per-cluster port assignment (py-1.15.0, initiative
`daemon-port-stability`).

These lock the anti-drift contract: a cluster_id ALWAYS resolves to the
same port across calls, distinct clusters never collide, and `_pick_port`
refuses to silently land on a port owned by a *different* live cluster.

In-process (no daemon boot): we redirect the machine-global registry file
into tmp_path and stub `_port_free` / `_probe_cluster_id` so the socket
world is deterministic.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
# DA-BOOTSTRAP-01 — _pick_port + the port registry moved to bootstrap.py;
# patch/call there (daemon re-exports them, but monkeypatching must target
# the module the function actually resolves its globals from).
import bootstrap as d  # type: ignore[import-not-found]  # noqa: E402


class _FakePaths:
    """Minimal stand-in: _pick_port only touches paths.port_file."""

    def __init__(self, tmp: Path) -> None:
        self.port_file = tmp / "port"


@pytest.fixture()
def registry(tmp_path, monkeypatch):
    """Point the global registry at tmp_path; default to an empty box where
    every port is free and nothing else answers /health."""
    reg = tmp_path / "ports.json"
    monkeypatch.setattr(d, "_PORT_REGISTRY_DIR", tmp_path, raising=False)
    monkeypatch.setattr(d, "_PORT_REGISTRY_FILE", reg, raising=False)
    monkeypatch.setattr(d, "_port_free", lambda p: True)
    monkeypatch.setattr(d, "_probe_cluster_id", lambda p: "")
    return reg


def _pick(tmp_path, cluster_id, cli=None, yaml=None):
    return d._pick_port(
        _FakePaths(tmp_path), cluster_id=cluster_id, cli_override=cli, yaml_port=yaml
    )


def test_sticky_same_port_across_calls(tmp_path, registry):
    first = _pick(tmp_path, "meshkore-main")
    second = _pick(tmp_path, "meshkore-main")
    third = _pick(tmp_path, "meshkore-main")
    assert first == second == third
    # persisted to the registry file, keyed by cluster_id
    assert d._registry_read()["meshkore-main"] == first


def test_distinct_clusters_get_distinct_ports(tmp_path, registry):
    a = _pick(tmp_path, "cavioca")
    b = _pick(tmp_path, "ikamiro")
    c = _pick(tmp_path, "meshkore-main")
    assert len({a, b, c}) == 3
    lo, hi = d.PORT_RANGE
    assert all(lo <= p <= hi for p in (a, b, c))


def test_cli_override_wins_and_becomes_sticky(tmp_path, registry):
    chosen = _pick(tmp_path, "cavioca", cli=5581)
    assert chosen == 5581
    # sticky: a later call with no override returns the override'd port
    assert _pick(tmp_path, "cavioca") == 5581


def test_yaml_port_seeds_first_assignment(tmp_path, registry):
    assert _pick(tmp_path, "cavioca", yaml=5585) == 5585


def test_anti_steal_reassigns_off_a_foreign_live_cluster(
    tmp_path, registry, monkeypatch
):
    # cavioca already owns 5570 (seed it deterministically)
    assert _pick(tmp_path, "cavioca", yaml=5570) == 5570
    # ikamiro's sticky port (5571) is now BUSY and held by a different cluster
    d._registry_write({"cavioca": 5570, "ikamiro": 5571})
    busy = {5571}
    monkeypatch.setattr(d, "_port_free", lambda p: p not in busy)
    monkeypatch.setattr(
        d, "_probe_cluster_id", lambda p: "cavioca" if p == 5571 else ""
    )
    got = _pick(tmp_path, "ikamiro")
    assert got != 5571  # never steals the foreign live port
    assert got != 5570  # nor cavioca's reserved port
    assert d._registry_read()["ikamiro"] == got  # reassignment persisted


def test_own_stale_instance_keeps_port(tmp_path, registry, monkeypatch):
    # meshkore-main owns 5572 but the port reads busy — held by ITS OWN id
    # (a dying instance / re-exec). We must keep the sticky port, not flee.
    d._registry_write({"meshkore-main": 5572})
    monkeypatch.setattr(d, "_port_free", lambda p: False)
    monkeypatch.setattr(d, "_probe_cluster_id", lambda p: "meshkore-main")
    assert _pick(tmp_path, "meshkore-main") == 5572
