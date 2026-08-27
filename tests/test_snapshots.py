"""Standard §20 (v19) — pre-modification file snapshots.

DAH1 (initiative `daemon-audit-hardening`). The standard has mandated this
surface since v19 and every project's CLAUDE.md preamble tells every agent that
POSTing to it before editing an existing file is non-negotiable — but the
daemon never implemented the route, so `POST /snapshots` answered
`{"error": "unknown route"}` and the operator's ability to inspect the pre-edit
state between two commits did not exist.

These tests pin the contract from the spec: the create → list → manifest →
raw-file → delete round-trip, the traversal refusals, the "newly created files
are exempt" carve-out from the agent contract, the auth gate (§20
scope_exclusions: never exposed to unauthenticated peers), and the daily-log
integration.
"""

from __future__ import annotations

import pytest

from conftest import Daemon


def _create(daemon: Daemon, **body):
    return daemon.post("/snapshots", json=body, headers=daemon.auth)


# ── auth (scope_exclusions: portal-token on every route) ─────────────────


@pytest.mark.parametrize(
    "method,path",
    [
        ("GET", "/snapshots"),
        ("GET", "/snapshots/anything"),
        ("POST", "/snapshots"),
    ],
)
def test_every_route_requires_the_portal_token(
    daemon: Daemon, method: str, path: str
) -> None:
    r = daemon.get(path) if method == "GET" else daemon.post(path, json={})
    assert r.status_code == 401, f"{method} {path} was reachable anonymously"


# ── create ───────────────────────────────────────────────────────────────


def test_create_copies_the_file_verbatim_and_returns_a_manifest(
    daemon: Daemon,
) -> None:
    target = daemon.root / ".meshkore" / "docs" / "thing.md"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text("BEFORE the edit\n")

    r = _create(
        daemon,
        paths=[".meshkore/docs/thing.md"],
        agent_id="a1",
        conv="c1",
        note="about to rewrite thing.md",
    )
    assert r.status_code == 201, r.text
    body = r.json()
    assert body["created"] is True
    bucket = body["id"]
    assert bucket and bucket[:8].isdigit(), "bucket id must start YYYYMMDD"
    entry = next(f for f in body["files"] if f["path"].endswith("thing.md"))
    assert "skipped" not in entry
    assert entry["size"] == len("BEFORE the edit\n")

    # The agent now makes its edit — the snapshot must still hold the BEFORE.
    target.write_text("AFTER the edit\n")
    raw = daemon.get(
        f"/snapshots/{bucket}/files/.meshkore/docs/thing.md", headers=daemon.auth
    )
    assert raw.status_code == 200
    assert raw.text == "BEFORE the edit\n"


def test_paths_is_required(daemon: Daemon) -> None:
    assert _create(daemon).status_code == 400
    assert _create(daemon, paths=[]).status_code == 400


def test_missing_file_is_skipped_not_fatal(daemon: Daemon) -> None:
    """The agent contract exempts NEWLY-CREATED files (no prior content), so a
    path that doesn't exist yet is an expected, itemised skip — not an error
    that would tempt an agent to stop calling the endpoint."""
    target = daemon.root / ".meshkore" / "docs" / "exists.md"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text("x\n")
    r = _create(daemon, paths=[".meshkore/docs/exists.md", "brand/new/file.ts"])
    assert r.status_code == 201
    files = {f["path"]: f for f in r.json()["files"]}
    assert "skipped" not in files[".meshkore/docs/exists.md"]
    assert files["brand/new/file.ts"]["skipped"] == "not an existing file"


@pytest.mark.parametrize(
    "bad", ["../../etc/passwd", "/etc/passwd", ".meshkore/../../outside.txt"]
)
def test_traversal_is_refused(daemon: Daemon, bad: str) -> None:
    r = _create(daemon, paths=[bad])
    assert r.status_code == 200, "nothing copied → no bucket, but a clear answer"
    assert r.json()["created"] is False
    assert r.json()["files"][0]["skipped"]


def test_nothing_to_copy_creates_no_bucket(daemon: Daemon) -> None:
    r = _create(daemon, paths=["does/not/exist.md"])
    assert r.status_code == 200
    assert r.json()["created"] is False
    assert r.json()["id"] is None
    assert daemon.get("/snapshots", headers=daemon.auth).json()["buckets"] == []


# ── list / manifest / delete ─────────────────────────────────────────────


def test_list_is_newest_first_and_delete_removes(daemon: Daemon) -> None:
    target = daemon.root / ".meshkore" / "docs" / "seq.md"
    target.parent.mkdir(parents=True, exist_ok=True)
    ids = []
    for i in range(3):
        target.write_text(f"rev {i}\n")
        r = _create(daemon, paths=[".meshkore/docs/seq.md"], note=f"rev{i}")
        assert r.status_code == 201, r.text
        ids.append(r.json()["id"])

    listed = daemon.get("/snapshots", headers=daemon.auth).json()["buckets"]
    assert [b["id"] for b in listed] == list(reversed(ids)), "newest first"
    assert listed[0]["file_count"] == 1

    manifest = daemon.get(f"/snapshots/{ids[0]}", headers=daemon.auth).json()
    assert manifest["id"] == ids[0]
    assert manifest["note"] == "rev0"

    d = daemon.client.delete(daemon.base + f"/snapshots/{ids[0]}", headers=daemon.auth)
    assert d.status_code == 200 and d.json()["deleted"] is True
    remaining = daemon.get("/snapshots", headers=daemon.auth).json()["buckets"]
    assert ids[0] not in [b["id"] for b in remaining]


def test_limit_is_honoured(daemon: Daemon) -> None:
    target = daemon.root / ".meshkore" / "docs" / "many.md"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text("x\n")
    for i in range(4):
        _create(daemon, paths=[".meshkore/docs/many.md"], note=f"n{i}")
    got = daemon.get("/snapshots?limit=2", headers=daemon.auth).json()["buckets"]
    assert len(got) == 2


def test_unknown_bucket_is_a_resource_404(daemon: Daemon) -> None:
    r = daemon.get("/snapshots/20260101-000000000-x-y-abcd", headers=daemon.auth)
    assert r.status_code == 404
    assert "unknown route" not in r.text, "must reach the handler, not fall through"


def test_percent_encoded_traversal_in_the_bucket_id_is_refused(
    daemon: Daemon,
) -> None:
    """A plain `../` is collapsed by any sane HTTP client before it leaves the
    machine; the shape that actually reaches a server is percent-encoded, and
    the handler unquotes it itself. `_BUCKET_RE` is what stops it."""
    r = daemon.get("/snapshots/%2e%2e%2f%2e%2e%2fetc", headers=daemon.auth)
    assert r.status_code == 404
    assert r.json()["error"] == "unknown snapshot bucket"


def test_raw_file_read_cannot_escape_the_bucket(daemon: Daemon) -> None:
    target = daemon.root / ".meshkore" / "docs" / "conf.md"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text("secret-ish\n")
    bucket = _create(daemon, paths=[".meshkore/docs/conf.md"]).json()["id"]
    r = daemon.get(f"/snapshots/{bucket}/files/../_manifest.json", headers=daemon.auth)
    assert r.status_code == 404


# ── daily-log integration (§20 daily_log_integration) ────────────────────


def test_create_appends_to_the_daily_log(daemon: Daemon) -> None:
    target = daemon.root / ".meshkore" / "docs" / "logged.md"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text("content\n")
    r = _create(daemon, paths=[".meshkore/docs/logged.md"], agent_id="scribe")
    bucket, date = r.json()["id"], r.json()["date"]
    entry = (daemon.root / ".meshkore" / "log" / f"{date}.md").read_text()
    assert f"snapshot {bucket}" in entry
    assert "scribe" in entry
    assert ".meshkore/docs/logged.md" in entry


# ── config (§20 config: cluster.yaml#snapshots.enabled) ──────────────────


def test_disabled_by_cluster_yaml(daemon: Daemon) -> None:
    cfg = daemon.root / ".meshkore" / "public" / "cluster.yaml"
    cfg.write_text(cfg.read_text() + "snapshots:\n  enabled: false\n")
    # cluster.yaml is re-read by the FS poller; give it a couple of ticks.
    import time

    deadline = time.time() + 15
    while time.time() < deadline:
        r = _create(daemon, paths=["anything.md"])
        if r.status_code == 403:
            assert r.json()["error"] == "snapshots_disabled"
            return
        time.sleep(1.0)
    pytest.fail("snapshots.enabled: false was never picked up")
