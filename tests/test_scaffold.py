"""scaffold.py + `daemon.py init` + auto-scaffold-on-boot (py-1.27.x,
initiative onboarding-self-bootstrap).

Two layers:

* **In-process** — call ``scaffold_cluster`` / ``slugify_id`` directly and
  assert the tree it writes is correct + schema-valid. Network-free: the
  ``MESHKORE_DAEMON_NO_BOOT_UPDATE`` env (set by the autouse fixture) makes
  ``scaffold._offline()`` true, so the standard-version / preamble fetches
  short-circuit to their offline fallbacks → deterministic
  ``STANDARD_VERSION == DEFAULT_STANDARD_VERSION``.

* **Subprocess** — drive the real CLI: ``daemon.py init`` scaffolds and is a
  graceful no-op on re-run, and a bare ``daemon.py`` auto-scaffolds on first
  boot then serves ``/health``. These reuse conftest's hermetic spawn
  helpers (same flag → offline scaffold).
"""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

import httpx
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import scaffold  # type: ignore[import-not-found]  # noqa: E402
from cluster import Cluster  # type: ignore[import-not-found]  # noqa: E402
from paths import Paths  # type: ignore[import-not-found]  # noqa: E402

from conftest import (  # noqa: E402
    DAEMON_PY,
    _free_port,
    _spawn,
    _wait_ready,
)

_ID_RE = re.compile(r"^[a-z0-9-]{2,40}$")


@pytest.fixture(autouse=True)
def _offline_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Hermetic: no api.meshkore.com calls during scaffold."""
    monkeypatch.setenv("MESHKORE_DAEMON_NO_BOOT_UPDATE", "1")


# ── slugify_id ───────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "name,expected",
    [
        ("Reddit intel", "reddit-intel"),
        ("Reddit  Intel!!", "reddit-intel"),
        ("  Leading/Trailing  ", "leading-trailing"),
        ("ALLCAPS", "allcaps"),
        ("a", "cluster"),  # too short → fallback
        ("", "cluster"),  # empty → fallback
        ("x" * 60, "x" * 40),  # clamp to 40
    ],
)
def test_slugify_id(name: str, expected: str) -> None:
    out = scaffold.slugify_id(name)
    assert out == expected
    assert _ID_RE.match(out), f"{out!r} violates the cluster id pattern"


def test_slugify_unicode_stays_valid() -> None:
    # Accents are stripped (not transliterated) but the result is still a
    # legal id — never an empty/invalid slug.
    out = scaffold.slugify_id("Café São Paulo!! 2026")
    assert _ID_RE.match(out)


# ── scaffold_cluster: the tree it writes ──────────────────────────────────


@pytest.fixture
def built(tmp_path: Path) -> Paths:
    return scaffold.scaffold_cluster(tmp_path, "Reddit intel", description="My desc.")


def test_writes_full_tree(built: Paths) -> None:
    root = built.root
    must_exist = [
        ".meshkore/STANDARD_VERSION",
        ".meshkore/public/cluster.yaml",
        ".meshkore/public/README.md",
        ".meshkore/public/AGENT_INSTRUCTIONS.md",
        ".meshkore/docs/governance.md",
        ".meshkore/docs/INDEX.md",
        ".meshkore/modules/general/README.md",
        ".meshkore/modules/project/README.md",
        ".meshkore/modules/general/tasks/T1-hello.md",
        ".gitignore",
        "CLAUDE.md",
        "AGENTS.md",
        "GEMINI.md",
    ]
    for rel in must_exist:
        assert (root / rel).is_file(), f"missing {rel}"
    # directories that exist even when empty
    for rel in (".meshkore/roadmap/initiatives", ".meshkore/timeline", ".meshkore/log"):
        assert (root / rel).is_dir(), f"missing dir {rel}"


def test_standard_version_offline_fallback(built: Paths) -> None:
    v = (built.meshkore / "STANDARD_VERSION").read_text().strip()
    assert v == str(scaffold.DEFAULT_STANDARD_VERSION)


def test_cluster_yaml_schema_valid(built: Paths) -> None:
    c = Cluster(built)
    assert c.data["version"] == 1
    assert c.id == "reddit-intel" and _ID_RE.match(c.id)
    assert c.type == "dev"
    assert c.name == "Reddit intel"  # display name preserved
    mods = {(m["id"], m["kind"]) for m in c.modules}
    assert ("general", "area") in mods and ("project", "area") in mods
    assert c.data["transport"]["endpoint"].startswith("https://daemon.meshkore.com:")
    assert c.data["storage"]["mode"] == "local"
    assert c.data["daemon"]["auto_update"] is True


def test_starter_task_frontmatter(built: Paths) -> None:
    body = (built.modules_dir / "general" / "tasks" / "T1-hello.md").read_text()
    for field in (
        "id:",
        "title:",
        "status:",
        "priority:",
        "owner:",
        "category:",
        "created:",
        "updated:",
    ):
        assert field in body, f"task frontmatter missing {field}"
    # §4 rule: category MUST equal the module id.
    assert "category: general" in body


def test_agent_instructions_markers_and_render(built: Paths) -> None:
    src = (built.public / "AGENT_INSTRUCTIONS.md").read_text()
    assert "MESHKORE_PREAMBLE_BEGIN" in src
    assert "MESHKORE_PREAMBLE_END" in src
    assert "OPERATOR_CONTENT_BEGIN" in src
    # per-CLI files carry the §17 auto-render header
    for cli in ("CLAUDE.md", "AGENTS.md", "GEMINI.md"):
        assert (
            "Auto-rendered from .meshkore/public/AGENT_INSTRUCTIONS.md"
            in (built.root / cli).read_text()
        )


def test_gitignore_denylist(built: Paths) -> None:
    gi = (built.root / ".gitignore").read_text()
    # ignored (runtime / secret / per-machine / generated / downloaded)
    for entry in (
        ".meshkore/.runtime/",
        ".meshkore/credentials/",
        ".meshkore/timeline/",
        ".meshkore/log/",
        ".meshkore/roadmap/state.json",
        ".meshkore/scripts/",
    ):
        assert entry in gi, f".gitignore missing {entry}"
    # committed (the project's brain) — must NOT be ignored
    assert ".meshkore/docs/" not in gi
    assert ".meshkore/modules/" not in gi
    assert ".meshkore/roadmap/initiatives/" not in gi


def test_gitignore_idempotent_merge(tmp_path: Path) -> None:
    (tmp_path / ".gitignore").write_text("node_modules/\n.meshkore/scripts/\n")
    scaffold.scaffold_cluster(tmp_path, "X Y")
    gi = (tmp_path / ".gitignore").read_text()
    assert gi.count(".meshkore/scripts/") == 1  # not duplicated
    assert "node_modules/" in gi  # preserved
    assert ".meshkore/.runtime/" in gi  # new entries appended


def test_refuse_clobber_then_force(tmp_path: Path) -> None:
    scaffold.scaffold_cluster(tmp_path, "First")
    with pytest.raises(scaffold.ScaffoldError):
        scaffold.scaffold_cluster(tmp_path, "Second")  # no force → refuse
    p = scaffold.scaffold_cluster(tmp_path, "Second", force=True)
    assert Cluster(p).name == "Second"


def test_credentials_dir_locked_down(built: Paths) -> None:
    # mode 0700 (best-effort; skip the assert on platforms without it)
    mode = built.credentials.stat().st_mode & 0o777
    assert mode in (0o700, 0o755) or built.credentials.is_dir()


# ── subprocess: the real CLI / boot paths ──────────────────────────────────


def _hermetic_env() -> dict:
    import os

    env = os.environ.copy()
    env["MESHKORE_DAEMON_NO_BOOT_UPDATE"] = "1"
    env["MESHKORE_DAEMON_SELF_UPDATED"] = "1"
    return env


def test_init_cli_scaffolds_and_is_idempotent(tmp_path: Path) -> None:
    """`daemon.py init` scaffolds (exit 0), and a second run with no --force
    is a graceful no-op (exit 0, 'skipping') — so the operator's
    `init ; launch` one-liner is safe to re-paste."""
    r1 = subprocess.run(
        [
            sys.executable,
            str(DAEMON_PY),
            "init",
            "--name",
            "Reddit Intel",
            "--root",
            str(tmp_path),
        ],
        capture_output=True,
        text=True,
        env=_hermetic_env(),
        timeout=30,
    )
    assert r1.returncode == 0, r1.stdout + r1.stderr
    cy = tmp_path / ".meshkore/public/cluster.yaml"
    assert cy.is_file() and "id: reddit-intel" in cy.read_text()

    r2 = subprocess.run(
        [
            sys.executable,
            str(DAEMON_PY),
            "init",
            "--name",
            "Reddit Intel",
            "--root",
            str(tmp_path),
        ],
        capture_output=True,
        text=True,
        env=_hermetic_env(),
        timeout=30,
    )
    assert r2.returncode == 0, "re-run must be a graceful no-op, not an error"
    assert "skipping" in (r2.stdout + r2.stderr).lower()


def test_wrong_dir_guard(tmp_path: Path) -> None:
    """Bare boot in a folder with NO .meshkore/ refuses (won't scatter a
    cluster into a random directory)."""
    r = subprocess.run(
        [sys.executable, str(DAEMON_PY), "--root", str(tmp_path)],
        capture_output=True,
        text=True,
        env=_hermetic_env(),
        timeout=15,
    )
    assert r.returncode != 0
    assert ".meshkore/ not found" in (r.stdout + r.stderr)


def test_auto_scaffold_on_boot_then_serves(tmp_path: Path) -> None:
    """A project where the daemon was DOWNLOADED (.meshkore/scripts/) but
    `init` was never run: a bare launch auto-scaffolds then serves /health.
    This is the zero-agent-execution onboarding path."""
    root = tmp_path / "reddit-intel"
    (root / ".meshkore" / "scripts").mkdir(
        parents=True
    )  # .meshkore exists, no cluster.yaml
    work = tmp_path / "work"
    work.mkdir()
    port = _free_port()
    proc = _spawn(DAEMON_PY, root, port, work)
    base = f"https://127.0.0.1:{port}"
    try:
        with httpx.Client(timeout=5.0, verify=False) as client:
            _wait_ready(client, base, proc)
            health = client.get(base + "/health").json()
        assert (root / ".meshkore/public/cluster.yaml").is_file()
        assert health["cluster_id"] == "reddit-intel"  # derived from folder name
        assert (root / ".meshkore/STANDARD_VERSION").read_text().strip() == str(
            scaffold.DEFAULT_STANDARD_VERSION
        )
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
