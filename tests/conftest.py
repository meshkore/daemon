"""Test infrastructure for the meshkore daemon.

Two abstractions only:

* ``cluster(...)`` — write a minimal ``.meshkore/`` tree to ``tmp_path``.
  Three preset shapes (``empty``, ``populated``, ``heavy_archive``) cover
  every characterization test.
* ``daemon`` — spawn the daemon as a subprocess against a built cluster,
  yield a ``Daemon`` handle (port, token, http base, http client). Tear
  down on exit.

HTTPS with verify=False — the daemon auto-loads its ``tls/`` sibling,
so we keep production's transport contract without faking a cert. The
``test_prompts`` file imports daemon.py in-process for the pure-function
helpers — no subprocess needed there.
"""

from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterator

import httpx
import pytest

DAEMON_PY = Path(__file__).resolve().parents[1] / "daemon.py"
BUNDLE_PY = Path(__file__).resolve().parents[1] / "dist" / "daemon.py"
TOKEN = "test-token-deterministic-please"  # baked into fixtures; never a real secret


# ── Cluster builders ─────────────────────────────────────────────────────


def _write(root: Path, rel: str, body: str | bytes) -> Path:
    """Write a file under ``root``, creating parent dirs. Bytes-aware."""
    p = root / rel
    p.parent.mkdir(parents=True, exist_ok=True)
    if isinstance(body, bytes):
        p.write_bytes(body)
    else:
        p.write_text(body)
    return p


def _scaffold(root: Path, *, cid: str = "test-cluster") -> None:
    """Skeleton every cluster needs: cluster.yaml + portal token."""
    _write(
        root,
        ".meshkore/public/cluster.yaml",
        f'version: 1\nid: {cid}\ntype: dev\nname: "Test {cid}"\n',
    )
    _write(root, ".meshkore/credentials/portal-token", TOKEN)
    (root / ".meshkore/credentials/portal-token").chmod(0o600)


def cluster_empty(root: Path) -> Path:
    """No initiatives, no convs, no timeline. Tests the bootstrap path."""
    _scaffold(root, cid="empty")
    return root


def cluster_populated(root: Path) -> Path:
    """Realistic mid-life cluster: 2 initiatives, 3 tasks, 2 convs (1 archived),
    1 timeline file with a couple of chat events."""
    _scaffold(root, cid="populated")
    _write(
        root,
        ".meshkore/roadmap/initiatives/alpha.md",
        "---\nid: alpha\ntitle: Alpha\nstatus: active\nmodules: [daemon]\n---\n# Alpha\n",
    )
    _write(
        root,
        ".meshkore/roadmap/initiatives/beta.md",
        "---\nid: beta\ntitle: Beta\nstatus: next\nmodules: [webapp]\n---\n# Beta\n",
    )
    _write(
        root,
        ".meshkore/modules/daemon/tasks/T1.md",
        "---\nid: T1\ntitle: T1\nstatus: next\ncategory: daemon\ninitiative: alpha\n---\n",
    )
    _write(
        root,
        ".meshkore/modules/daemon/tasks/T2.md",
        "---\nid: T2\ntitle: T2\nstatus: next\ncategory: daemon\ninitiative: alpha\n---\n",
    )
    _write(
        root,
        ".meshkore/modules/webapp/tasks/T3.md",
        "---\nid: T3\ntitle: T3\nstatus: next\ncategory: webapp\ninitiative: beta\n---\n",
    )
    # Standard v14 §3.5 context tree — a couple of canonical files +
    # one decisions/ folder entry so /context has a non-trivial shape.
    _write(
        root,
        ".meshkore/context/overview.md",
        "---\ntitle: Overview\nupdated: 2026-06-06\nstatus: stable\n---\n"
        "# Overview\n\nA test cluster. Local-first multi-agent cockpit.\n",
    )
    _write(
        root,
        ".meshkore/context/stack.md",
        "---\ntitle: Stack\nupdated: 2026-06-06\n---\n# Stack\n\nPython daemon + SolidJS cockpit.\n",
    )
    _write(
        root,
        ".meshkore/context/decisions/README.md",
        "---\ntitle: Decisions\n---\n# Decisions\n\nNewest first.\n",
    )
    _write(
        root,
        ".meshkore/context/decisions/2026-06-06-pick-python.md",
        "---\ntitle: Daemon is Python\nupdated: 2026-06-06\nstatus: stable\n---\n"
        "**Context**: needed a daemon.\n\n**Decision**: Python.\n",
    )
    # conv_meta sidecar + archive
    _write(
        root,
        ".meshkore/.runtime/conv_meta.json",
        json.dumps(
            {
                "conv-a": {"agent_type": "custom"},
                "conv-b": {"agent_type": "custom"},
            }
        ),
    )
    _write(
        root,
        ".meshkore/.runtime/archives.json",
        json.dumps(
            {
                "version": 1,
                "archived": {
                    "conv-b": {"archived_at": "2026-06-01T00:00:00Z", "by": "test"},
                },
            }
        ),
    )
    # one timeline file with chat events for both convs
    timeline = [
        {
            "type": "chat.user",
            "conv": "conv-a",
            "ts": "2026-06-10T10:00:00Z",
            "text": "hi",
        },
        {
            "type": "chat.assistant.final",
            "conv": "conv-a",
            "ts": "2026-06-10T10:00:05Z",
            "text": "yo",
        },
        {
            "type": "chat.user",
            "conv": "conv-b",
            "ts": "2026-05-30T10:00:00Z",
            "text": "old",
        },
    ]
    _write(
        root,
        ".meshkore/timeline/2026-06-10.jsonl",
        "\n".join(json.dumps(ev) for ev in timeline) + "\n",
    )
    return root


def cluster_heavy_archive(root: Path) -> Path:
    """100 archived convs + 5 live. Stresses chat_convs() — the function
    that hung on ikamiro 2026-06-10."""
    _scaffold(root, cid="heavy-archive")
    meta = {f"conv-{i:03d}": {"agent_type": "custom"} for i in range(105)}
    archived = {
        f"conv-{i:03d}": {"archived_at": "2026-05-01T00:00:00Z", "by": "bulk"}
        for i in range(100)
    }
    _write(root, ".meshkore/.runtime/conv_meta.json", json.dumps(meta))
    _write(
        root,
        ".meshkore/.runtime/archives.json",
        json.dumps({"version": 1, "archived": archived}),
    )
    return root


BUILDERS: dict[str, Callable[[Path], Path]] = {
    "empty": cluster_empty,
    "populated": cluster_populated,
    "heavy_archive": cluster_heavy_archive,
}


@pytest.fixture
def cluster(tmp_path: Path) -> Callable[[str], Path]:
    """Factory: ``cluster('populated')`` returns a freshly-built root path."""

    def _build(name: str) -> Path:
        if name not in BUILDERS:
            raise KeyError(f"unknown fixture cluster: {name}")
        return BUILDERS[name](tmp_path)

    return _build


# ── Daemon spawn ─────────────────────────────────────────────────────────


def _free_port() -> int:
    """Bind 0.0.0.0:0, read the OS-assigned port, release. Race-prone in
    theory, fine in practice for tests; the daemon's own port-lock will
    fail loudly if another process grabbed it in the gap."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


@dataclass
class Daemon:
    proc: subprocess.Popen
    port: int
    token: str
    base: str  # e.g. "http://127.0.0.1:5573"
    root: Path
    client: httpx.Client

    def get(self, path: str, **kw: Any) -> httpx.Response:
        return self.client.get(self.base + path, **kw)

    def post(self, path: str, **kw: Any) -> httpx.Response:
        return self.client.post(self.base + path, **kw)

    @property
    def auth(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self.token}"}


def _spawn(daemon_py: Path, root: Path, port: int, work: Path) -> subprocess.Popen:
    """Start the daemon at its source path so coverage.py tracks the
    same file the tests assert against. The daemon auto-detects its
    ``tls/`` sibling, so tests run over HTTPS with ``verify=False`` —
    no production cert dance, no path-mapping in coverage."""
    env = os.environ.copy()
    env["MESHKORE_DAEMON_NO_BOOT_UPDATE"] = "1"  # hermetic — no CDN calls in tests
    env["MESHKORE_DAEMON_SELF_UPDATED"] = "1"  # belt-and-braces — block re-exec
    # D-TEST-ISO-01 — keep the sticky port registry in the test workdir so
    # the suite never writes the operator's real ~/.meshkore/ports.json.
    env["MESHKORE_PORTS_FILE"] = str(work / "ports.json")
    # Coverage in subprocess: tests/sitecustomize.py calls
    # coverage.process_startup() when COVERAGE_PROCESS_START is set. Adding
    # tests/ to PYTHONPATH gets sitecustomize.py imported automatically.
    tests_dir = Path(__file__).resolve().parent
    cov_cfg = tests_dir.parent / "pyproject.toml"
    env["COVERAGE_PROCESS_START"] = str(cov_cfg)
    env["PYTHONPATH"] = str(tests_dir) + os.pathsep + env.get("PYTHONPATH", "")
    proc = subprocess.Popen(
        [sys.executable, str(daemon_py), "--port", str(port), "--root", str(root)],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    return proc


def _wait_ready(
    client: httpx.Client, base: str, proc: subprocess.Popen, timeout: float = 8.0
) -> None:
    """Poll /health until 200 or timeout. ``client`` has verify=False so
    the daemon's TLS handshake to a 127.0.0.1 origin works."""
    deadline = time.time() + timeout
    last_err: Exception | None = None
    while time.time() < deadline:
        if proc.poll() is not None:
            out = proc.stdout.read() if proc.stdout else ""  # type: ignore[union-attr]
            raise RuntimeError(f"daemon exited early: {out}")
        try:
            r = client.get(base + "/health", timeout=1.0)
            if r.status_code == 200:
                return
        except httpx.HTTPError as e:
            last_err = e
        time.sleep(0.1)
    raise TimeoutError(f"daemon never returned 200 on /health; last={last_err}")


@pytest.fixture
def daemon(
    cluster: Callable[[str], Path], request: pytest.FixtureRequest
) -> Iterator[Daemon]:
    """Spawn the daemon against a 'populated' cluster by default. Override
    with ``@pytest.mark.cluster('empty')`` etc."""
    marker = request.node.get_closest_marker("cluster")
    name = marker.args[0] if marker else "populated"
    target = request.node.get_closest_marker("target")
    daemon_py = BUNDLE_PY if (target and target.args[0] == "bundle") else DAEMON_PY
    root = cluster(name)
    port = _free_port()
    # The daemon serves HTTPS when its tls/ bundle is present (it ships
    # with one). Tests use verify=False — the bundle's cert is for
    # daemon.meshkore.com but the daemon binds to 127.0.0.1. Production
    # TLS contract preserved; tests don't need a separate code path.
    base = f"https://127.0.0.1:{port}"
    work = root.parent / "daemon-work"
    work.mkdir(parents=True, exist_ok=True)
    proc = _spawn(daemon_py, root, port, work)
    try:
        with httpx.Client(timeout=5.0, verify=False) as client:
            _wait_ready(client, base, proc)
            yield Daemon(
                proc=proc, port=port, token=TOKEN, base=base, root=root, client=client
            )
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=2)
